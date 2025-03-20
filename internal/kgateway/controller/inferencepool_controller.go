package controller

import (
	"context"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	infextv1a2 "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/deployer"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/wellknown"
)

type inferencePoolReconciler struct {
	cli            client.Client
	scheme         *runtime.Scheme
	controllerName string
	deployer       *deployer.Deployer
}

func (r *inferencePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("inferencepool", req.NamespacedName)
	log.V(1).Info("reconciling request", "request", req)

	pool := new(infextv1a2.InferencePool)
	if err := r.cli.Get(ctx, req.NamespacedName, pool); err != nil {
		log.V(0).Info("InferencePool does not exist; skipping reconciliation")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// List the routes referencing this pool.
	var routeList gwv1.HTTPRouteList
	if err := r.cli.List(ctx, &routeList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{InferencePoolField: pool.Name},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Build the new InferencePool status entries, if needed.
	newStatus := r.buildStatus(routeList.Items, pool)
	// No HTTPRoutes reference the pool, so remove status. TODO [danehans]: Selectivly remove status
	// when https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/489 is resolved.
	if newStatus == nil {
		updated := removeStatuses(pool.Status.Parents)
		pool.Status.Parents = updated
		if err := r.cli.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}

		// Cleanup managed k8s resources for the pool.
		if err := r.deployer.CleanupClusterScopedResources(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.cli.Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// Merge newStatus back into the overall Parents array.
	changed, newPoolStatus := updatePoolStatus(pool.Status.Parents, newStatus)
	merged := mergeStatuses(pool.Status.Parents, newPoolStatus, r.controllerName)

	// The status has changed, update status on the object.
	if changed {
		pool.Status.Parents = merged
		if err := r.cli.Status().Update(ctx, pool); err != nil {
			log.Error(err, "failed to update InferencePool status")
			return ctrl.Result{}, err
		}
		log.V(1).Info("Updated InferencePool status")
	} else {
		log.V(1).Info("No status change needed for InferencePool")
	}

	// Get the endpoint picker resources to deploy.
	objs, err := r.deployer.GetEndpointPickerObjs(pool)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Deploy the endpoint picker resources.
	log.Info("Ensuring endpoint picker is deployed for InferencePool")
	err = r.deployer.DeployObjs(ctx, objs)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func mergeStatuses(
	oldStatus []infextv1a2.PoolStatus,
	newStatus []infextv1a2.PoolStatus,
	controllerName string,
) []infextv1a2.PoolStatus {
	// Remove old references.
	filtered := removeStatuses(oldStatus)
	return append(filtered, newStatus...)
}

func (r *inferencePoolReconciler) buildStatus(routes []gwv1.HTTPRoute, pool *infextv1a2.InferencePool) *infextv1a2.PoolStatus {
	for _, route := range routes {
		for _, p := range route.Status.Parents {
			if p.ControllerName == gwv1.GatewayController(r.controllerName) {
				ns := route.Namespace
				if p.ParentRef.Namespace != nil && *p.ParentRef.Namespace != "" {
					ns = string(*p.ParentRef.Namespace)
				}
				gwName := string(p.ParentRef.Name)

				return &infextv1a2.PoolStatus{
					GatewayRef: corev1.ObjectReference{
						APIVersion: gwv1.GroupVersion.String(),
						Kind:       wellknown.GatewayKind,
						Namespace:  ns,
						Name:       gwName,
					},
					Conditions: []metav1.Condition{
						{
							Type:               string(infextv1a2.InferencePoolConditionAccepted),
							Status:             metav1.ConditionTrue,
							Reason:             string(infextv1a2.InferencePoolReasonAccepted),
							Message:            "Referenced by an HTTPRoute accepted by this Gateway",
							ObservedGeneration: pool.Generation,
							LastTransitionTime: metav1.Now(),
						},
					},
				}
			}
		}
	}
	return nil
}

// removeStatuses removes all PoolStatus entries corresponding to
// the given controller from `parents` and returns the new filtered slice.
func removeStatuses(parents []infextv1a2.PoolStatus) []infextv1a2.PoolStatus {
	var out []infextv1a2.PoolStatus
	for _, ps := range parents {
		// TODO [danehans]: Upstream needd to fix status to ref an HTTPRoute:
		// https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/489
		out = append(out, ps)
	}
	return out
}

// updatePoolStatus compares the current pool status entries with the newly computed PoolStatus.
// If they differ, current is modified accorto include newStatus and returns. If no change is needed,
// false and the current unmodified pool status is returned.
func updatePoolStatus(current []infextv1a2.PoolStatus, newStatus *infextv1a2.PoolStatus) (bool, []infextv1a2.PoolStatus) {
	if newStatus == nil {
		return false, current
	}

	// Find a matching GatewayRef.
	foundIndex := -1
	for i, ps := range current {
		if ps.GatewayRef.Kind == newStatus.GatewayRef.Kind &&
			ps.GatewayRef.Namespace == newStatus.GatewayRef.Namespace &&
			ps.GatewayRef.Name == newStatus.GatewayRef.Name {
			foundIndex = i
			break
		}
	}

	if foundIndex == -1 {
		// No entry for this GatewayRef, so append the newly created one.
		return true, append(current, *newStatus)
	}

	// Fields to ignrore when performing a status condition comparison.
	cmpOptions := []cmp.Option{
		cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime", "ObservedGeneration"),
	}

	// If found, compare them and only replace if they differ (aside from ignored fields).
	if !cmp.Equal(current[foundIndex], *newStatus, cmpOptions...) {
		updated := make([]infextv1a2.PoolStatus, len(current))
		copy(updated, current)
		updated[foundIndex] = *newStatus
		return true, updated
	}

	// Statuses are the same, so no change.
	return false, current
}
