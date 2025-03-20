package controller

import (
	"context"
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	managedPools   map[types.NamespacedName]struct{} 
	parentGateways map[types.NamespacedName]struct{}
}

/*func (r *inferencePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("inferencepool", req.NamespacedName)
	log.V(1).Info("reconciling request", "request", req)

	pool := new(infextv1a2.InferencePool)
	if err := r.cli.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if pool.GetDeletionTimestamp() != nil {
		log.Info("Removing endpoint picker for InferencePool", "name", pool.Name, "namespace", pool.Namespace)

		// TODO [danehans]: EPP should use role and rolebinding RBAC: https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/224
		if err := r.deployer.CleanupClusterScopedResources(ctx, pool); err != nil {
			delete(r.managedPools, req.NamespacedName)
			return ctrl.Result{}, err
		}

		// Remove the finalizer.
		pool.Finalizers = slices.DeleteFunc(pool.Finalizers, func(s string) bool {
			return s == wellknown.InferencePoolFinalizer
		})

		// Delete the InferencePool from the cache.
		delete(r.managedPools, req.NamespacedName)

		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present for the InferencePool.
	if err := r.deployer.EnsureFinalizer(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

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

    // Mark the pool as active
    r.managedPools[req.NamespacedName] = struct{}{}

	log.V(1).Info("reconciled request", "request", req)

	return ctrl.Result{}, nil
}*/

func (r *inferencePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    log := log.FromContext(ctx).WithValues("inferencepool", req.NamespacedName)
    log.V(1).Info("reconciling request", "request", req)

    pool := new(infextv1a2.InferencePool)
    if err := r.cli.Get(ctx, req.NamespacedName, pool); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    if pool.GetDeletionTimestamp() != nil {
		log.Info("Removing endpoint picker for InferencePool", "name", pool.Name, "namespace", pool.Namespace)

		// TODO [danehans]: EPP should use role and rolebinding RBAC: https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/224
		if err := r.deployer.CleanupClusterScopedResources(ctx, pool); err != nil {
			delete(r.managedPools, req.NamespacedName)
			return ctrl.Result{}, err
		}

		// Remove the finalizer.
		pool.Finalizers = slices.DeleteFunc(pool.Finalizers, func(s string) bool {
			return s == wellknown.InferencePoolFinalizer
		})

		// Delete the InferencePool from the cache.
		// TODO [danehans] Update the parentGateways cache, if needed.
		delete(r.managedPools, req.NamespacedName)

        return ctrl.Result{}, nil
    }

    // Ensure the finalizer is present.
    if err := r.deployer.EnsureFinalizer(ctx, pool); err != nil {
        return ctrl.Result{}, err
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
    newOurStatuses, err := r.buildStatuses(routeList.Items, pool)
    if err != nil {
        return ctrl.Result{}, err
    }

    // 4) If we have zero references for our controller, consider cleaning up your resources,
    //    but do NOT blow away pool.Status.Parents entirely—only remove your entries:
    if len(newOurStatuses) == 0 {
        // No routes reference the pool for our controller => remove *our* statuses
        // from pool.Status.Parents, but keep statuses from other controllers.

        updated := removeStatusesForController(pool.Status.Parents, r.controllerName)
        if !parentsEqual(pool.Status.Parents, updated) {
            pool.Status.Parents = updated
            if err := r.cli.Status().Update(ctx, pool); err != nil {
                return ctrl.Result{}, err
            }
        }

        // Then do your cluster-scoped resource cleanup, ...
        if err := r.deployer.CleanupClusterScopedResources(ctx, pool); err != nil {
            return ctrl.Result{}, err
        }

        delete(r.activePools, req.NamespacedName)
        return ctrl.Result{}, nil
    }

    // 5) Merge newOurStatuses back into the overall Parents array.
    merged := mergeStatusesForController(pool.Status.Parents, newOurStatuses, r.controllerName)

    // 6) If changed, update pool.Status.
    if !parentsEqual(pool.Status.Parents, merged) {
        pool.Status.Parents = merged
        if err := r.cli.Status().Update(ctx, pool); err != nil {
            log.Error(err, "failed to update InferencePool status")
            return ctrl.Result{}, err
        }
        log.Info("Updated InferencePool status")
    } else {
        log.V(1).Info("No status change needed for InferencePool")
    }

    // 7) Deploy your endpoint picker resources, etc.
    if err := r.DeployEndpointPicker(ctx, pool); err != nil {
        return ctrl.Result{}, err
    }

    // Mark it active
    r.managedPools[req.NamespacedName] = struct{}{}

    return ctrl.Result{}, nil
}

func mergeStatusesForController(
    oldStatus []infextv1a2.PoolStatus,
    newStatus []infextv1a2.PoolStatus,
    controllerName string,
) []infextv1a2.PoolStatus {
    // Remove old references that match our controller.
    filtered := removeStatusesForController(oldStatus, controllerName)
    return append(filtered, newStatus...)
}

func (r *inferencePoolReconciler) removeStatuses(parents []infextv1a2.PoolStatus, controllerName string) []infextv1a2.PoolStatus {
    var out []infextv1a2.PoolStatus
	for _, p := range parents{
		if p.GatewayRef.APIVersion == gwv1.GroupVersion.Version &&
		p.GatewayRef.Kind == wellknown.GatewayKind {
			nn := types.NamespacedName{
				Namespace: p.GatewayRef.Namespace,
				Name:      p.GatewayRef.Name,
			}
			if _, ok := r.parentGateways[nn]; !ok {
				out = append(out, p)
			}
		}
		pool.Status.Parents = pStatus
		if err := r.cli.Status().Update(ctx, pool); err != nil {
			return err
		}
		log.Info("Updated InferencePool status")
	}
    return out
}

func  (r *inferencePoolReconciler) isOurController(ps infextv1a2.PoolStatus, controllerName string) bool {

    return (ps.GatewayRef.Kind == "Gateway")
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
                            Type:   string(infextv1a2.InferencePoolConditionAccepted),
                            Status: metav1.ConditionTrue,
                            Reason: string(infextv1a2.InferencePoolReasonAccepted),
                            Message: "Referenced by an HTTPRoute accepted by this Gateway",
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

/*func (r *inferencePoolReconciler) processStatus(ctx context.Context, pool *infextv1a2.InferencePool) error {
	log := log.FromContext(ctx).WithValues("name", pool.Name, "namespace", pool.Namespace)

	// Use the registered index to list HTTPRoutes that reference this pool.
	var routeList gwv1.HTTPRouteList
	if err := r.cli.List(ctx, &routeList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{InferencePoolField: pool.Name},
	); err != nil {
		return err
	}

	// If no HTTPRoutes reference the pool, clean-up and skip reconciliation.
	if len(routeList.Items) == 0 {
		log.Info("No HTTPRoutes reference this InferencePool; cleaning up status and cluster-scoped resources.")

		// If any parents are set, remove them.
		pStatus := []infextv1a2.PoolStatus{}
		for _, p := range pool.Status.Parents{
			if p.GatewayRef.APIVersion == gwv1.GroupVersion.Version &&
			p.GatewayRef.Kind == wellknown.GatewayKind {
				nn := types.NamespacedName{
					Namespace: p.GatewayRef.Namespace,
					Name:      p.GatewayRef.Name,
				}
				if _, ok := r.parentGateways[nn]; !ok {
					pStatus = append(pStatus, p)
				}
			}
			pool.Status.Parents = pStatus
			if err := r.cli.Status().Update(ctx, pool); err != nil {
				return err
			}
			log.Info("Updated InferencePool status")
		}

		// Clean up cluster-scoped resources.
		if err := r.deployer.CleanupClusterScopedResources(ctx, pool); err != nil {
			return err
		}

		// Remove the pool from the cache.
		delete(r.actmanagedPoolsypes.NamespacedName{Namespace: pool.Namespace, Name: pool.Name})

		return nil
	}

	// Compute a new single PoolStatus, if any.
	newStatus, err := r.buildStatus(routeList.Items, pool)
	if err != nil {
		return err
	}

	// Update pool.Status.Parents only if needed.
	changed, updatedParents := updatePoolStatus(pool.Status.Parents, newStatus)
	if changed {
		pool.Status.Parents = updatedParents
		if err := r.cli.Status().Update(ctx, pool); err != nil {
			return err
		}
		log.Info("Updated InferencePool status")
	} else {
		log.Info("No status change needed for InferencePool")
	}

	return nil
}*/

// buildStatus returns a single PoolStatus if any HTTPRoute is managed by our controllerName
// and references the InferencePool. If no references are found for our controller, nil is returned.
func (r *inferencePoolReconciler) buildStatus(routes []gwv1.HTTPRoute, pool *infextv1a2.InferencePool) (*infextv1a2.PoolStatus, error) {
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
							Message:            "Referenced by an HTTPRoute accepted by the parentRef Gateway",
							ObservedGeneration: pool.Generation,
							LastTransitionTime: metav1.NewTime(time.Now()),
						},
					},
				}, nil
			}
		}
	}

	// No references to our controllerName found.
	return nil, nil
}

// updatePoolStatus compares the newly computed PoolStatus to the current pool status entries.
// If they differ, current is modified accordingly and returns. If no change is needed, false and
// current (unmodified) pool status is returned.
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
