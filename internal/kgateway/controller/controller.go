package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	infextv1a2 "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/kgateway-dev/kgateway/v2/api/v1alpha1"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/deployer"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/wellknown"
)

const (
	GatewayClassField = "spec.gatewayClassName"
	// GatewayParamsField is the field name used for indexing Gateway objects.
	GatewayParamsField = "gateway-params"
	// InferencePoolField is the field name used for indexing HTTPRoute objects.
	InferencePoolField = "inferencepool-index"
)

// TODO [danehans]: Refactor so controller config is organized into shared and Gateway/InferencePool-specific controllers.
type GatewayConfig struct {
	Mgr manager.Manager

	Dev            bool
	ControllerName string
	AutoProvision  bool

	ControlPlane            deployer.ControlPlaneInfo
	IstioIntegrationEnabled bool
}

func NewBaseGatewayController(ctx context.Context, cfg GatewayConfig) error {
	log := log.FromContext(ctx)
	log.V(5).Info("starting gateway controller", "controllerName", cfg.ControllerName)

	controllerBuilder := &controllerBuilder{
		cfg: cfg,
		reconciler: &controllerReconciler{
			cli:    cfg.Mgr.GetClient(),
			scheme: cfg.Mgr.GetScheme(),
		},
	}

	return run(ctx,
		controllerBuilder.watchGwClass,
		controllerBuilder.watchGw,
		controllerBuilder.addIndexes,
	)
}

type InferencePoolConfig struct {
	Mgr          manager.Manager
	InferenceExt *deployer.InferenceExtInfo
}

func NewBaseInferencePoolController(ctx context.Context, poolCfg *InferencePoolConfig, gwCfg *GatewayConfig) error {
	log := log.FromContext(ctx)
	log.V(5).Info("starting inferencepool controller", "controllerName", gwCfg.ControllerName)

	// TODO [danehans]: Make GatewayConfig optional since Gateway and InferencePool are independent controllers.
	controllerBuilder := &controllerBuilder{
		cfg:     *gwCfg,
		poolCfg: poolCfg,
		reconciler: &controllerReconciler{
			cli:    poolCfg.Mgr.GetClient(),
			scheme: poolCfg.Mgr.GetScheme(),
		},
	}

	return run(ctx, controllerBuilder.watchInferencePool)
}

func run(ctx context.Context, funcs ...func(ctx context.Context) error) error {
	for _, f := range funcs {
		if err := f(ctx); err != nil {
			return err
		}
	}
	return nil
}

type controllerBuilder struct {
	cfg        GatewayConfig
	poolCfg    *InferencePoolConfig
	reconciler *controllerReconciler
}

func (c *controllerBuilder) addIndexes(ctx context.Context) error {
	if err := c.cfg.Mgr.GetFieldIndexer().IndexField(ctx, &gwv1.Gateway{}, GatewayParamsField, gatewayToParams); err != nil {
		return err
	}
	if err := c.cfg.Mgr.GetFieldIndexer().IndexField(ctx, &gwv1.Gateway{}, GatewayClassField, gatewayToClass); err != nil {
		return err
	}
	return nil
}

// gatewayToParams is an IndexerFunc that gets a GatewayParameters name from a Gateway.
// It checks the Gateway's spec.infrastructure.parametersRef, or returns an empty
// slice when it's not set.
func gatewayToParams(obj client.Object) []string {
	gw, ok := obj.(*gwv1.Gateway)
	if !ok {
		panic(fmt.Sprintf("wrong type %T provided to indexer. expected Gateway", obj))
	}
	infrastructureRef := gw.Spec.Infrastructure
	if infrastructureRef != nil && infrastructureRef.ParametersRef != nil {
		return []string{infrastructureRef.ParametersRef.Name}
	}
	return []string{}
}

// gatewayToClass is an IndexerFunc that lists a Gateways that use a given className
func gatewayToClass(obj client.Object) []string {
	gw, ok := obj.(*gwv1.Gateway)
	if !ok {
		panic(fmt.Sprintf("wrong type %T provided to indexer. expected Gateway", obj))
	}
	return []string{string(gw.Spec.GatewayClassName)}
}

func (c *controllerBuilder) watchGw(ctx context.Context) error {
	// setup a deployer
	log := log.FromContext(ctx)

	log.Info("creating gateway deployer", "ctrlname", c.cfg.ControllerName, "server", c.cfg.ControlPlane.XdsHost, "port", c.cfg.ControlPlane.XdsPort)
	d, err := deployer.NewDeployer(c.cfg.Mgr.GetClient(), &deployer.Inputs{
		ControllerName:          c.cfg.ControllerName,
		Dev:                     c.cfg.Dev,
		IstioIntegrationEnabled: c.cfg.IstioIntegrationEnabled,
		ControlPlane:            c.cfg.ControlPlane,
	})
	if err != nil {
		return err
	}

	gvks, err := d.GetGvksToWatch(ctx)
	if err != nil {
		return err
	}

	buildr := ctrl.NewControllerManagedBy(c.cfg.Mgr).
		// Don't use WithEventFilter here as it also filters events for Owned objects.
		For(&gwv1.Gateway{}, builder.WithPredicates(
			// TODO(stevenctl) investigate perf implications of filtering in Reconcile
			// the tricky part is we want to check a relationship of gateway -> gatewayclass -> controller name
			predicate.Or(
				predicate.AnnotationChangedPredicate{},
				predicate.GenerationChangedPredicate{},
			),
		))

	// watch for changes in GatewayParameters and enqueue Gateways that use them
	cli := c.cfg.Mgr.GetClient()
	buildr.Watches(&v1alpha1.GatewayParameters{}, handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			gwpName := obj.GetName()
			gwpNamespace := obj.GetNamespace()
			// look up the Gateways that are using this GatewayParameters object
			var gwList gwv1.GatewayList
			err := cli.List(ctx, &gwList, client.InNamespace(gwpNamespace), client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector(GatewayParamsField, gwpName)})
			if err != nil {
				log.Error(err, "could not list Gateways using GatewayParameters", "gwpNamespace", gwpNamespace, "gwpName", gwpName)
				return []reconcile.Request{}
			}
			// requeue each Gateway that is using this GatewayParameters object
			reqs := make([]reconcile.Request, 0, len(gwList.Items))
			for _, gw := range gwList.Items {
				reqs = append(reqs, reconcile.Request{
					NamespacedName: client.ObjectKeyFromObject(&gw),
				})
			}
			return reqs
		}))
	// watch for gatewayclasses managed by our controller and enqueue related gateways
	buildr.Watches(
		&gwv1.GatewayClass{},
		handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			gc, ok := obj.(*gwv1.GatewayClass)
			if !ok {
				return nil
			}
			var gwList gwv1.GatewayList
			if err := c.cfg.Mgr.GetClient().List(
				ctx,
				&gwList,
				client.MatchingFields{GatewayClassField: gc.Name},
			); err != nil {
				log.Error(err, "failed listing GatewayClasses in predicate")
				return nil
			}
			reqs := make([]reconcile.Request, 0, len(gwList.Items))
			for _, gw := range gwList.Items {
				reqs = append(reqs,
					reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&gw)},
				)
			}
			return reqs
		}),
		builder.WithPredicates(
			predicate.NewPredicateFuncs(func(o client.Object) bool {
				gc, ok := o.(*gwv1.GatewayClass)
				return ok && gc.Spec.ControllerName == gwv1.GatewayController(c.cfg.ControllerName)
			}),
			predicate.GenerationChangedPredicate{},
		),
	)

	for _, gvk := range gvks {
		obj, err := c.cfg.Mgr.GetScheme().New(gvk)
		if err != nil {
			return err
		}
		clientObj, ok := obj.(client.Object)
		if !ok {
			return fmt.Errorf("object %T is not a client.Object", obj)
		}
		log.Info("watching gvk as gateway child", "gvk", gvk)
		// unless it's a service, we don't care about the status
		var opts []builder.OwnsOption
		if shouldIgnoreStatusChild(gvk) {
			opts = append(opts, builder.WithPredicates(predicate.GenerationChangedPredicate{}))
		}
		buildr.Owns(clientObj, opts...)
	}

	return buildr.Complete(&gatewayReconciler{
		cli:            c.cfg.Mgr.GetClient(),
		scheme:         c.cfg.Mgr.GetScheme(),
		controllerName: c.cfg.ControllerName,
		autoProvision:  c.cfg.AutoProvision,
		deployer:       d,
	})
}

type routeEventHandler struct{}

func (h *routeEventHandler) Create(
	ctx context.Context,
	e event.TypedCreateEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	route, ok := e.Object.(*gwv1.HTTPRoute)
	if !ok || route == nil {
		return
	}
	for _, req := range poolsFromRoute(route) {
		q.Add(req)
	}
}

func (h *routeEventHandler) Update(
	ctx context.Context,
	e event.TypedUpdateEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	oldRoute, okOld := e.ObjectOld.(*gwv1.HTTPRoute)
	newRoute, okNew := e.ObjectNew.(*gwv1.HTTPRoute)
	if !okOld || !okNew {
		return
	}
	// Enqueue Pools for the old object
	for _, req := range poolsFromRoute(oldRoute) {
		q.Add(req)
	}
	// Enqueue Pools for the new object
	for _, req := range poolsFromRoute(newRoute) {
		q.Add(req)
	}
}

func (h *routeEventHandler) Delete(
	ctx context.Context,
	e event.TypedDeleteEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	route, ok := e.Object.(*gwv1.HTTPRoute)
	if !ok || route == nil {
		return
	}
	for _, req := range poolsFromRoute(route) {
		q.Add(req)
	}
}

func (h *routeEventHandler) Generic(
	ctx context.Context,
	e event.TypedGenericEvent[client.Object],
	q workqueue.TypedRateLimitingInterface[reconcile.Request],
) {
	// no-op
}

func (c *controllerBuilder) addHTTPRouteIndexes(ctx context.Context) error {
	return c.cfg.Mgr.GetFieldIndexer().IndexField(ctx, new(gwv1.HTTPRoute), InferencePoolField, httpRouteInferencePoolIndex)
}

func httpRouteInferencePoolIndex(obj client.Object) []string {
	route, ok := obj.(*gwv1.HTTPRoute)
	if !ok {
		return nil
	}

	// Build a ns/name list of pool references for the HTTPRoute.
	var poolRefs []string
	for _, rule := range route.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			if ref.Kind != nil && *ref.Kind == wellknown.InferencePoolKind {
				ns := route.GetNamespace()
				if ref.Namespace != nil {
					ns = string(*ref.Namespace)
				}
				poolRef := types.NamespacedName{
					Namespace: ns,
					Name:      string(ref.Name),
				}
				poolRefs = append(poolRefs, poolRef.String())
			}
		}
	}

	return poolRefs
}

// watchInferencePool adds a watch on InferencePool and HTTPRoute objects
// to trigger reconciliation.
func (c *controllerBuilder) watchInferencePool(ctx context.Context) error {
	log := log.FromContext(ctx)
	log.Info("creating inference extension deployer", "controller", c.cfg.ControllerName)

	if err := c.addHTTPRouteIndexes(ctx); err != nil {
		return fmt.Errorf("failed to register HTTPRoute index: %w", err)
	}

	d, err := deployer.NewDeployer(c.cfg.Mgr.GetClient(), &deployer.Inputs{
		ControllerName:     c.cfg.ControllerName,
		InferenceExtension: c.poolCfg.InferenceExt,
	})
	if err != nil {
		return err
	}

	buildr := ctrl.NewControllerManagedBy(c.cfg.Mgr).
		For(&infextv1a2.InferencePool{}).
		Watches(&gwv1.HTTPRoute{}, &routeEventHandler{})

	gvks, err := d.GetGvksToWatch(ctx)
	if err != nil {
		return err
	}
	for _, gvk := range gvks {
		obj, err := c.cfg.Mgr.GetScheme().New(gvk)
		if err != nil {
			return err
		}
		clientObj, ok := obj.(client.Object)
		if !ok {
			return fmt.Errorf("object %T is not a client.Object", obj)
		}
		buildr.Owns(clientObj, builder.WithPredicates(predicate.GenerationChangedPredicate{}))
	}

	r := &inferencePoolReconciler{
		cli:            c.cfg.Mgr.GetClient(),
		scheme:         c.cfg.Mgr.GetScheme(),
		controllerName: c.cfg.ControllerName,
		deployer:       d,
	}

	return buildr.Complete(r)
}

func routeToPoolRequests(oldObj, newObj *gwv1.HTTPRoute) []ctrl.Request {
	var reqs []ctrl.Request
	oldRefs := poolRefsFromHTTPRoute(oldObj)
	newRefs := poolRefsFromHTTPRoute(newObj)

	unique := make(map[types.NamespacedName]struct{})
	for _, ref := range oldRefs {
		unique[ref] = struct{}{}
	}
	for _, ref := range newRefs {
		unique[ref] = struct{}{}
	}
	for k := range unique {
		reqs = append(reqs, ctrl.Request{NamespacedName: k})
	}
	return reqs
}

func poolsFromRoute(route *gwv1.HTTPRoute) []ctrl.Request {
	refs := poolRefsFromHTTPRoute(route)
	reqs := make([]ctrl.Request, 0, len(refs))
	for _, r := range refs {
		reqs = append(reqs, ctrl.Request{NamespacedName: r})
	}
	return reqs
}

func poolRefsFromHTTPRoute(route *gwv1.HTTPRoute) []types.NamespacedName {
	var out []types.NamespacedName
	for _, rule := range route.Spec.Rules {
		for _, ref := range rule.BackendRefs {
			// Only care about references of kind InferencePool
			if ref.Kind != nil && *ref.Kind == wellknown.InferencePoolKind {
				ns := route.Namespace
				if ref.Namespace != nil {
					ns = string(*ref.Namespace)
				}
				out = append(out, types.NamespacedName{
					Namespace: ns,
					Name:      string(ref.Name),
				})
			}
		}
	}
	return out
}

func shouldIgnoreStatusChild(gvk schema.GroupVersionKind) bool {
	// avoid triggering on pod changes that update deployment status
	return gvk.Kind == "Deployment"
}

func (c *controllerBuilder) watchGwClass(_ context.Context) error {
	return ctrl.NewControllerManagedBy(c.cfg.Mgr).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
			// we only care about GatewayClasses that use our controller name
			if gwClass, ok := object.(*gwv1.GatewayClass); ok {
				return gwClass.Spec.ControllerName == gwv1.GatewayController(c.cfg.ControllerName)
			}
			return false
		})).
		For(&gwv1.GatewayClass{}).
		Complete(reconcile.Func(c.reconciler.ReconcileGatewayClasses))
}

type controllerReconciler struct {
	cli    client.Client
	scheme *runtime.Scheme
}

func (r *controllerReconciler) ReconcileGatewayClasses(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("gwclass", req.NamespacedName)

	gwclass := &gwv1.GatewayClass{}
	if err := r.cli.Get(ctx, req.NamespacedName, gwclass); err != nil {
		// NOTE: if this reconciliation is a result of a DELETE event, this err will be a NotFound,
		// therefore we will return a nil error here and thus skip any additional reconciliation below.
		// At the time of writing this comment, the retrieved GWClass object is only used to update the status,
		// so it should be fine to return here, because there's no status update needed on a deleted resource.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling gateway class")

	// mark it as accepted:
	acceptedCondition := metav1.Condition{
		Type:               string(gwv1.GatewayClassConditionStatusAccepted),
		Status:             metav1.ConditionTrue,
		Reason:             string(gwv1.GatewayClassReasonAccepted),
		ObservedGeneration: gwclass.Generation,
		// no need to set LastTransitionTime, it will be set automatically by SetStatusCondition
	}
	meta.SetStatusCondition(&gwclass.Status.Conditions, acceptedCondition)

	// TODO: This should actually check the version of the CRDs in the cluster to be 100% sure
	supportedVersionCondition := metav1.Condition{
		Type:               string(gwv1.GatewayClassConditionStatusSupportedVersion),
		Status:             metav1.ConditionTrue,
		ObservedGeneration: gwclass.Generation,
		Reason:             string(gwv1.GatewayClassReasonSupportedVersion),
	}
	meta.SetStatusCondition(&gwclass.Status.Conditions, supportedVersionCondition)

	if err := r.cli.Status().Update(ctx, gwclass); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("updated gateway class status")

	return ctrl.Result{}, nil
}
