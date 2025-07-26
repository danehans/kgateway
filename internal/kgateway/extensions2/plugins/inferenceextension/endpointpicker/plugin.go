package endpointpicker

import (
	"context"
	"fmt"
	//"strings"
	"time"
    //"crypto/sha1"
    //"encoding/hex"
    //"sort"

	headertometadata "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/header_to_metadata/v3"
	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoyroutev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	envoytlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	upstreamsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	//"google.golang.org/protobuf/types/known/wrapperspb"
	"google.golang.org/protobuf/types/known/structpb"
	skubeclient "istio.io/istio/pkg/config/schema/kubeclient"
	"istio.io/istio/pkg/kube/krt"
	"knative.dev/pkg/network"
	//discoveryv1 "k8s.io/api/discovery/v1"
	//corev1 "k8s.io/api/core/v1"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/krtcollections"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	infextv1a2 "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"
	"sigs.k8s.io/gateway-api-inference-extension/client-go/clientset/versioned"

	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/extensions2/common"
	extplug "github.com/kgateway-dev/kgateway/v2/internal/kgateway/extensions2/plugin"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/plugins"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/utils"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/wellknown"
	"github.com/kgateway-dev/kgateway/v2/pkg/logging"
	"github.com/kgateway-dev/kgateway/v2/pkg/pluginsdk/ir"
	"github.com/kgateway-dev/kgateway/v2/pkg/reports"
)

// Derived from upstream Gateway API Inference Extension defaults (testdata/envoy.yaml).
const DefaultExtProcMaxRequests = 40000

var (
	logger = logging.New("plugin/inference-epp")

	inferencePoolGVK = wellknown.InferencePoolGVK
	inferencePoolGVR = inferencePoolGVK.GroupVersion().WithResource("inferencepools")
)

func registerTypes(cli versioned.Interface) {
	skubeclient.Register[*infextv1a2.InferencePool](
		inferencePoolGVR,
		inferencePoolGVK,
		func(c skubeclient.ClientGetter, namespace string, o metav1.ListOptions) (runtime.Object, error) {
			return cli.InferenceV1alpha2().InferencePools(namespace).List(context.Background(), o)
		},
		func(c skubeclient.ClientGetter, namespace string, o metav1.ListOptions) (watch.Interface, error) {
			return cli.InferenceV1alpha2().InferencePools(namespace).Watch(context.Background(), o)
		},
	)
}

func NewPlugin(ctx context.Context, commonCol *common.CommonCollections) *extplug.Plugin {
	// Create the inference extension clientset.
	cli, err := versioned.NewForConfig(commonCol.Client.RESTConfig())
	if err != nil {
		logger.Error("failed to create inference extension client", "error", err)
		return nil
	}

	// Register the InferencePool type to enable dynamic object translation.
	registerTypes(cli)



	return NewPluginFromCollections(ctx, commonCol, poolCol)
}

func NewPluginFromCollections(
	ctx context.Context,
	commonCol *common.CommonCollections,
	poolCol krt.Collection[*infextv1a2.InferencePool],
) *extplug.Plugin {
	// The InferencePool group kind used by the BackendObjectIR and the ContributesBackendObjectIRs plugin.
	gk := schema.GroupKind{
		Group: inferencePoolGVK.Group,
		Kind:  inferencePoolGVK.Kind,
	}

	poolBackend := krt.NewCollection(poolCol, func(kctx krt.HandlerContext, pool *infextv1a2.InferencePool) *ir.BackendObjectIR {
		eps := resolvePoolEndpoints(pool, commonCol.Pods)
		logger.Debug("NewPluginFromCollections resolvePoolEndpoints()", "endpoints", eps)

		/*hash := func(eps Endpoints) string {
			addrs := make([]string, 0, len(eps))
			for _, ep := range eps {
				addrs = append(addrs, ep.String())
			}
			sort.Strings(addrs)
			h := sha1.Sum([]byte(strings.Join(addrs, ",")))
			return hex.EncodeToString(h[:])
		}(eps)*/

		// Validate the InferencePool and create the associated IR.
		irPool := newInferencePool(pool, eps)
		//irPool.Endpoints = endpoints
		logger.Debug("NewPluginFromCollections backendCol() irPool.endpoints", "endpoints", eps)
		errs := validatePool(pool, commonCol.Services)
		// The infpool spec does not state the pod selector must resolve pods.
		/*if len(eps) == 0 {
			errs = append(errs, fmt.Errorf("no endpoints found for InferencePool"))
		}*/
		if errs != nil {
			// If there are validation errors, add them to the IR.
			irPool.setErrors(errs)
		}

		be := buildBackendObjIrFromPool(irPool)
		//be.ExtraKey = hash

		return be
	}, commonCol.KrtOpts.ToOptions("InferencePoolBackends")...)

	epsCol := krt.NewCollection(poolBackend, func(kctx krt.HandlerContext, be ir.BackendObjectIR) *ir.EndpointsForBackend {
		// Initialize a stub Cluster with the same name that Translator will use:
		stub := &envoyclusterv3.Cluster{
			Name: be.ClusterName(),
		}
		logger.Debug("NewPluginFromCollections epsCol() calling processPoolBackendObjIR()", "BackendObjectIR", be)
		return processPoolBackendObjIR(ctx, be, stub)
	},
		commonCol.KrtOpts.ToOptions("InferencePoolEndpoints")...,
	)

	/*epSliceCol := krt.WrapClient(kclient.NewFiltered[*discoveryv1.EndpointSlice](
		commonCol.Client,
		kclient.Filter{ObjectFilter: commonCol.Client.ObjectFilter()},
	), commonCol.KrtOpts.ToOptions("EndpointSlice")...)

	inputs  := krtcollections.NewGlooK8sEndpointInputs(
				commonCol.Settings,
				commonCol.KrtOpts,
				epSliceCol,
				commonCol.Pods,
				poolBackend,
	)

	// Build a KRT collection that produces EndpointsForBackend
	epsCol := krtcollections.NewK8sEndpoints(ctx, inputs)*/

	// Create an index for each InferencePool backend keyed by namespace/name.
	poolByNN := krt.NewIndex(poolBackend, func(b ir.BackendObjectIR) []string {
		src := b.ObjectSource
		if wellknown.IsInferencePoolGK(src.Group, src.Kind) {
			return []string{fmt.Sprintf("%s/%s", src.Namespace, src.Name)}
		}
		return nil
	})
	//poolByNN := poolBackend

	policyCol := krt.NewCollection(poolBackend, func(kctx krt.HandlerContext, be ir.BackendObjectIR) *ir.PolicyWrapper {
		// Ensure the backend object is an InferencePool.
		irPool, ok := be.ObjIr.(*inferencePool)
		if !ok || irPool == nil {
			return nil
		}

		// Resolve endpoints from pods
		/*sel, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: irPool.podSelector})
		if err != nil {
			return nil
		}
		var endpoints []Endpoint
		for _, pod := range commonCol.Pods.List() {
			if pod.Namespace == be.Namespace {
				logger.Debug("NewPluginFromCollections policyCol matched pod for irPool.endpoints", "podName", pod.Name)
				podIP := pod.Address()
				if sel.Matches(labels.Set(pod.AugmentedLabels)) && podIP != "" {
					endpoints = append(endpoints, Endpoint{Address: podIP, Port: irPool.targetPort})
					logger.Debug("NewPluginFromCollections appended endpoint to irPool.endpoints", "podName", pod.Name)
				}
			}
		}*/

		// You no longer have to re-select pods: irPool.Endpoints is already set.
		logger.Debug("policyCol() sees irPool.endpoints", "endpoints", irPool.Endpoints)

		for _, ep := range irPool.Endpoints {
			logger.Debug("NewPluginFromCollections policyCol() endpoint from eps slice", "address", ep.Address, "Port", ep.Port)
		}

		// Validate the InferencePool and create the associated IR.
		/*irPool := newInferencePool(be)
		irPool.Endpoints = endpoints
		logger.Debug("NewPluginFromCollections policyCol() irPool.endpoints", "endpoints", irPool.Endpoints)
		errs := validatePool(be, svcCol)
		if len(endpoints) == 0 {
			errs = append(errs, fmt.Errorf("no endpoints found for InferencePool"))
		}
		if errs != nil {
			// If there are validation errors, add them to the IR.
			irPool.setErrors(errs)
		}*/

		// Create a PolicyWrapper IR representation from the given InferencePool.
		return &ir.PolicyWrapper{
			ObjectSource: ir.ObjectSource{
				Group:     gk.Group,
				Kind:      gk.Kind,
				Namespace: be.Namespace,
				Name:      be.Name,
			},
			Policy:   be.Obj.(*infextv1a2.InferencePool),
			PolicyIR: irPool,
		}
	})

	// Return a plugin that contributes a policy and backend.
	return &extplug.Plugin{
		ContributesBackends: map[schema.GroupKind]extplug.BackendPlugin{
			gk: {
				BackendInit: ir.BackendInit{
					InitBackend: func(ctx context.Context, be ir.BackendObjectIR, c *envoyclusterv3.Cluster) *ir.EndpointsForBackend {
						return processPoolBackendObjIR(ctx, be, c, commonCol.Pods)
					},
				},
				Backends:    poolBackend,
				Endpoints:   epsCol,
			},
		},
		ContributesPolicies: map[schema.GroupKind]extplug.PolicyPlugin{
			gk: {
				Name:     "endpoint-picker",
				Policies: policyCol,
				NewGatewayTranslationPass: func(ctx context.Context, tctx ir.GwTranslationCtx, reporter reports.Reporter) ir.ProxyTranslationPass {
					return newEndpointPickerPass(reporter, commonCol)
				},
			},
		},
		ContributesRegistration: map[schema.GroupKind]func(){
			gk: buildRegisterCallback(ctx, commonCol, poolBackend, poolByNN),
		},
	}
}

// resolvePoolEndpoints takes an InferencePool and a list of LocalityPods
// and returns the subset of Endpoints whose labels match the pool’s selector.
func resolvePoolEndpoints(
    pool *infextv1a2.InferencePool,
    pods krt.Collection[krtcollections.LocalityPod],
) Endpoints {
    sel, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
        MatchLabels: convertSelector(pool.Spec.Selector),
    })
    if err != nil {
        logger.Error("invalid selector on InferencePool", "pool", pool.Name, "err", err)
        return nil
    }
    allPods := pods.List()
    logger.Debug("resolvePoolEndpoints: total pods in cluster", "count", len(allPods))

    out := make(Endpoints, 0, len(allPods))
    for _, pod := range allPods {
        if pod.Namespace != pool.Namespace {
            continue
        }
        ip := pod.Address()
        if ip == "" {
            continue
        }
        if sel.Matches(labels.Set(pod.AugmentedLabels)) {
            ep := Endpoint{Address: ip, Port: pool.Spec.TargetPortNumber}
            out = append(out, ep)
            logger.Debug("resolvePoolEndpoints: matched pod", "pod", pod.Name, "endpoint", ep)
        }
    }
    logger.Debug("resolvePoolEndpoints: endpoints for pool", "pool", pool.Name, "endpoints", out)
    return out
}


func buildBackendObjIrFromPool(pool *inferencePool) *ir.BackendObjectIR {
	// Create a BackendObjectIR IR representation from the given InferencePool.
	/*objSrc := ir.ObjectSource{
		Kind:      gk.Kind,
		Group:     gk.Group,
		Namespace: pool.Namespace,
		Name:      pool.Name,
	}
	backend := ir.NewBackendObjectIR(objSrc, pool.Spec.TargetPortNumber, "")
	backend.Obj = pool
	backend.GvPrefix = "endpoint-picker"
	backend.CanonicalHostname = ""
	backend.ObjIr = irPool*/

	objSrc := ir.ObjectSource{
		Kind:      wellknown.InferencePoolGVK.Kind,
		Group:     wellknown.InferencePoolGVK.Group,
		Namespace: pool.obj.GetNamespace(),
		Name:      pool.obj.GetName(),
	}
	backend := ir.NewBackendObjectIR(objSrc, pool.targetPort, "")
	backend.GvPrefix = "endpoint-picker"
	backend.Obj = pool.obj
	backend.ObjIr = pool
	//backend.AppProtocol = ir.ParseAppProtocol(&svcProtocol)
	//backend.GvPrefix = "kube"
	// TODO: reevaluate knative dep, dedupe with pkg/utils/kubeutils/dns.go
	backend.CanonicalHostname = fmt.Sprintf("%s.%s.svc.%s", objSrc.Name, objSrc.Namespace, network.GetClusterDomainName())
	return &backend
}

/*func configureCluster(
    _ context.Context,
    _ ir.BackendObjectIR,
    out *envoyclusterv3.Cluster,
) *ir.EndpointsForBackend {
    out.ConnectTimeout = durationpb.New(5 * time.Second)
    out.ClusterDiscoveryType = &envoyclusterv3.Cluster_Type{Type: envoyclusterv3.Cluster_EDS}
    out.EdsClusterConfig = &envoyclusterv3.Cluster_EdsClusterConfig{
        EdsConfig: &envoycorev3.ConfigSource{
            ConfigSourceSpecifier: &envoycorev3.ConfigSource_Ads{},
            ResourceApiVersion:    envoycorev3.ApiVersion_V3,
        },
    }
    out.LbPolicy = envoyclusterv3.Cluster_ROUND_ROBIN
    out.LbSubsetConfig = &envoyclusterv3.Cluster_LbSubsetConfig{
        SubsetSelectors: []*envoyclusterv3.Cluster_LbSubsetConfig_LbSubsetSelector{{
            Keys: []string{"x-gateway-destination-endpoint"},
        }},
        FallbackPolicy: envoyclusterv3.Cluster_LbSubsetConfig_ANY_ENDPOINT,
    }

  // Construct an HttpProtocolOptions proto and put it into
  // TypedExtensionProtocolOptions under the well-known HTTP2 key.
  http2Opts := &upstreamsv3.HttpProtocolOptions{
      UpstreamProtocolOptions: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_{
          ExplicitHttpConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig{
              ProtocolConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
                  // empty options == default HTTP/2 behaviour
                  Http2ProtocolOptions: &envoycorev3.Http2ProtocolOptions{},
              },
          },
      },
  }
  anyHttp2, err := utils.MessageToAny(http2Opts)
  if err != nil {
      logger.Error("failed to marshal HTTP/2 options", "err", err)
  } else {
      out.TypedExtensionProtocolOptions = map[string]*anypb.Any{
          "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": anyHttp2,
      }
  }

    return nil
}*/

/*func configureCluster(
	_ context.Context,
	be ir.BackendObjectIR,
	out *envoyclusterv3.Cluster,
) *ir.EndpointsForBackend {
	// We only want to touch the *service-generated* cluster for our InferencePool.
	// The HTTPRoute.ApplyForBackend call rewrote the route to:
	//   kube_<namespace>_<poolName>-svc_<port>
	// so we detect that here and bail on all others.
	irPool, ok := be.ObjIr.(*inferencePool)
	if !ok || irPool == nil {
		return nil
	}

	svcName := fmt.Sprintf("%s-svc", irPool.obj.GetName())
	expectedName := fmt.Sprintf(
		"kube_%s_%s_%d",
		irPool.obj.GetNamespace(),
		svcName,
		irPool.targetPort,
	)
	if out.Name != expectedName {
		// Not our service cluster - leave it alone
		return nil
	}

	// Standard EDS setup
	out.ClusterDiscoveryType = &envoyclusterv3.Cluster_Type{
		Type: envoyclusterv3.Cluster_EDS,
	}
	out.EdsClusterConfig = &envoyclusterv3.Cluster_EdsClusterConfig{
		EdsConfig: &envoycorev3.ConfigSource{
			ResourceApiVersion: envoycorev3.ApiVersion_V3,
			ConfigSourceSpecifier: &envoycorev3.ConfigSource_Ads{
				Ads: &envoycorev3.AggregatedConfigSource{},
			},
		},
	}
	/*out.EdsClusterConfig = &envoyclusterv3.Cluster_EdsClusterConfig{
		EdsConfig: &envoycorev3.ConfigSource{
			ConfigSourceSpecifier: &envoycorev3.ConfigSource_Ads{},
			ResourceApiVersion:    envoycorev3.ApiVersion_V3,
		},
	}*/

	// Teach the Service-generated EDS cluster to subset‐LB on our header metadata
	/*out.LbSubsetConfig = &envoyclusterv3.Cluster_LbSubsetConfig{
		FallbackPolicy: envoyclusterv3.Cluster_LbSubsetConfig_ANY_ENDPOINT,
		SubsetSelectors: []*envoyclusterv3.Cluster_LbSubsetConfig_LbSubsetSelector{{
			Keys: []string{"x-gateway-destination-endpoint"},
		}},
	}

	// Preserve your HTTP/2 protocol options upstream
	http2Opts := &upstreamsv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
					Http2ProtocolOptions: &envoycorev3.Http2ProtocolOptions{},
				},
			},
		},
	}
	anyHttp2, err := utils.MessageToAny(http2Opts)
	if err != nil {
		logger.Error("failed to marshal HTTP/2 options", "err", err)
	} else {
		out.TypedExtensionProtocolOptions = map[string]*anypb.Any{
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": anyHttp2,
		}
	}

	// From k8s plugin that uses eds cluster
	out.IgnoreHealthOnHostRemoval = true

	// We're not returning an EndpointsForBackend here,
	// because the real Service cluster is EDS‐driven by krt.
	return nil
}*/

// endpointPickerPass implements ir.ProxyTranslationPass. It collects any references to IR inferencePools.
type endpointPickerPass struct {
	commonCols *common.CommonCollections
	// usedPools defines a map of IR inferencePools keyed by NamespacedName.
	usedPools map[types.NamespacedName]*inferencePool
	ir.UnimplementedProxyTranslationPass

	reporter reports.Reporter
}

var _ ir.ProxyTranslationPass = &endpointPickerPass{}

func newEndpointPickerPass(
	reporter reports.Reporter,
	commonCols *common.CommonCollections,
) ir.ProxyTranslationPass {
	return &endpointPickerPass{
		usedPools:  make(map[types.NamespacedName]*inferencePool),
		reporter:   reporter,
		commonCols: commonCols,
	}
}

func (p *endpointPickerPass) Name() string {
	return "endpoint-picker"
}

// ApplyForBackend updates the Envoy route for each InferencePool-backed HTTPRoute.
func (p *endpointPickerPass) ApplyForBackend(
	ctx context.Context,
	pCtx *ir.RouteBackendContext,
	in ir.HttpBackend,
	out *envoyroutev3.Route,
) error {
	if p == nil || pCtx == nil || pCtx.Backend == nil {
		return nil
	}

	// Ensure the backend object is an InferencePool.
	irPool, ok := pCtx.Backend.ObjIr.(*inferencePool)
	if !ok || irPool == nil {
		return nil
	}

	// Store this pool in our map, keyed by NamespacedName.
	nn := types.NamespacedName{
		Namespace: irPool.obj.GetNamespace(),
		Name:      irPool.obj.GetName(),
	}
	p.usedPools[nn] = irPool

	// Tell the EPP which subset of endpoints to choose from
	pool, ok := irPool.obj.(*infextv1a2.InferencePool)
	if pool == nil || !ok {
		return nil
	}

	// Ensure RouteAction is initialized.
	if out.GetRoute() == nil {
		out.Action = &envoyroutev3.Route_Route{
			Route: &envoyroutev3.RouteAction{},
		}
	}

	// get the RouteAction
	ra := out.GetRoute()

    // Single source of truth: always use the backend’s ClusterName
    ra.ClusterSpecifier = &envoyroutev3.RouteAction_Cluster{
        Cluster: pCtx.Backend.ClusterName(),
    }

	if ra.GetMetadataMatch() == nil {
		ra.MetadataMatch = &envoycorev3.Metadata{}
	}
	if ra.MetadataMatch.FilterMetadata == nil {
		ra.MetadataMatch.FilterMetadata = make(map[string]*structpb.Struct)
	}

	eps := resolvePoolEndpoints(pool, p.commonCols.Pods)
    if len(eps) == 0 {
        return fmt.Errorf("no endpoints found for InferencePool %s/%s", pool.Namespace, pool.Name)
    }
	logger.Debug("ApplyForBackend endpoints from resolvePoolEndpoints()", "endpoints", eps)
    irPool.Endpoints = eps

	// TODO use the following instead of ^ eps if lgs shows endpoints from the IR.
    //epsFromIR := irPool.Endpoints
    //logger.Debug("ApplyForBackend endpoints from pool IR", "endpoints", epsFromIR)

	// Add the endpoint subset list header - dynamic metadata should match
	// See https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/docs/proposals/004-endpoint-picker-protocol
	/*pCtx.RequestHeadersToAdd = append(pCtx.RequestHeadersToAdd, &envoycorev3.HeaderValueOption{
		Header: &envoycorev3.HeaderValue{
			Key:   "x-gateway-destination-endpoint-subset",
			Value: eps.ToString(),
		},
	})*/

	// Tell the EPP which subset of endpoints to choose from
	vs := make([]*structpb.Value, 0, len(eps))
	for _, ep := range eps {
		vs = append(vs, structpb.NewStringValue(ep.String()))
	}
	hintStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"x-gateway-destination-endpoint-subset": {
				Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: vs}},
			},
		},
	}
	//ra.MetadataMatch.FilterMetadata["envoy.lb.subset_hint"] = hintStruct

	// Attach the subset hint under the "envoy.lb.subset_hint" namespace
	pCtx.TypedFilterConfig.AddTypedConfig(
		wellknown.InfPoolTransformationFilterName,
		&envoycorev3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				"envoy.lb.subset_hint": hintStruct,
			},
		},
	)

	// Instead of our empty subset‐cluster (which never got endpoints),
	// route directly to the Service‐backed EDS cluster that krt already produces:
	/*svcName := fmt.Sprintf("%s-svc", irPool.objMeta.GetName())
	clusterName := fmt.Sprintf(
		"kube_%s_%s_%d",
		irPool.objMeta.GetNamespace(),
		svcName,
		irPool.targetPort,
	)
	out.GetRoute().ClusterSpecifier = &envoyroutev3.RouteAction_Cluster{
		Cluster: clusterName,
	}*/

	// Route into the subset cluster whose endpoints we populated from the pool ref'd pods.
	/*ra.ClusterSpecifier = &envoyroutev3.RouteAction_Cluster{
		Cluster: fmt.Sprintf("inference-pool-%s-%s", nn.Namespace, nn.Name),
	}*/

	// Build the route-level ext_proc override that points to this pool's ext_proc cluster.
	override := &extprocv3.ExtProcPerRoute{
		Override: &extprocv3.ExtProcPerRoute_Overrides{
			Overrides: &extprocv3.ExtProcOverrides{
				GrpcService: &envoycorev3.GrpcService{
					Timeout: durationpb.New(10 * time.Second),
					TargetSpecifier: &envoycorev3.GrpcService_EnvoyGrpc_{
						EnvoyGrpc: &envoycorev3.GrpcService_EnvoyGrpc{
							ClusterName: clusterNameExtProc(
								irPool.obj.GetName(),
								irPool.obj.GetNamespace(),
							),
							Authority: authorityForPool(irPool),
						},
					},
				},
			},
		},
	}

	// Attach per-route override to typed_per_filter_config.
	pCtx.TypedFilterConfig.AddTypedConfig(wellknown.InfPoolTransformationFilterName, override)

	return nil
}

// HttpFilters returns one ext_proc filter, using the well-known filter name.
func (p *endpointPickerPass) HttpFilters(ctx context.Context, fc ir.FilterChainCommon) ([]plugins.StagedHttpFilter, error) {
	if p == nil || len(p.usedPools) == 0 {
		return nil, nil
	}

	// Create a pool as placeholder for the static config
	tmpPool := &inferencePool{
		obj: &infextv1a2.InferencePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "placeholder-pool",
				Namespace: "placeholder-namespace",
			},
		},
		configRef: &service{
			ObjectSource: ir.ObjectSource{Name: "placeholder-service"},
			ports:        []servicePort{{name: "grpc", portNum: 9002}},
		},
	}

	// Static ExternalProcessor that will be overridden by ExtProcPerRoute
	extProcSettings := &extprocv3.ExternalProcessor{
		GrpcService: &envoycorev3.GrpcService{
			TargetSpecifier: &envoycorev3.GrpcService_EnvoyGrpc_{
				EnvoyGrpc: &envoycorev3.GrpcService_EnvoyGrpc{
					ClusterName: clusterNameExtProc(
						tmpPool.obj.GetName(),
						tmpPool.obj.GetNamespace(),
					),
					Authority: authorityForPool(tmpPool),
				},
			},
		},
		ProcessingMode: &extprocv3.ProcessingMode{
			RequestHeaderMode:   extprocv3.ProcessingMode_SEND,
			RequestBodyMode:     extprocv3.ProcessingMode_FULL_DUPLEX_STREAMED,
			RequestTrailerMode:  extprocv3.ProcessingMode_SEND,
			ResponseBodyMode:    extprocv3.ProcessingMode_FULL_DUPLEX_STREAMED,
			ResponseHeaderMode:  extprocv3.ProcessingMode_SEND,
			ResponseTrailerMode: extprocv3.ProcessingMode_SEND,
		},
		MessageTimeout:   durationpb.New(5 * time.Second),
		FailureModeAllow: true,
		MetadataOptions: &extprocv3.MetadataOptions{
			ForwardingNamespaces: &extprocv3.MetadataOptions_MetadataNamespaces{
				Untyped: []string{"envoy.lb.subset_hint"},
			},
			ReceivingNamespaces: &extprocv3.MetadataOptions_MetadataNamespaces{
				Untyped: []string{"envoy.lb"},
			},
		},
	}

	extProcFilter, err := plugins.NewStagedFilter(
		wellknown.InfPoolTransformationFilterName,
		extProcSettings,
		plugins.BeforeStage(plugins.AuthNStage),
	)
	if err != nil {
		return nil, err
	}

	htm := &headertometadata.Config{
		RequestRules: []*headertometadata.Config_Rule{{
			Header: "x-gateway-destination-endpoint",
			OnHeaderPresent: &headertometadata.Config_KeyValuePair{
				MetadataNamespace: "envoy.lb",
				Key:               "x-gateway-destination-endpoint",
				Type:              headertometadata.Config_STRING,
			},
			Remove: false,
		}},
	}
	htmFilter, _ := plugins.NewStagedFilter(
		"envoy.filters.http.header_to_metadata",
		htm,
		plugins.BeforeStage(plugins.RouteStage),
	)


	return []plugins.StagedHttpFilter{extProcFilter, htmFilter}, nil
}

// ResourcesToAdd returns the ext_proc clusters for all used InferencePools.
func (p *endpointPickerPass) ResourcesToAdd(ctx context.Context) ir.Resources {
	if p == nil || len(p.usedPools) == 0 {
		return ir.Resources{}
	}
	var clusters []*envoyclusterv3.Cluster
	for _, pool := range p.usedPools {
		if c := buildExtProcCluster(pool); c != nil {
			clusters = append(clusters, c)
		}
	}
	return ir.Resources{Clusters: clusters}
}

// processPoolBackendObjIR builds an EDS + subset-LB cluster per InferencePool.
func processPoolBackendObjIR(
	ctx context.Context,
	in ir.BackendObjectIR,
	out *envoyclusterv3.Cluster,
	//podsCol krt.Collection[krtcollections.LocalityPod],
) *ir.EndpointsForBackend {
	irPool, ok := in.ObjIr.(*inferencePool)
	if !ok || irPool == nil {
		logger.Debug("BackendObjectIR is not a IR pool or the pool is nil")
		return nil
	}

	/*pool, ok := in.Obj.(*infextv1a2.InferencePool)
	if !ok || pool == nil {
		logger.Debug("BackendObjectIR is not a pool or the pool is nil")
		return nil
	}

    irPool.Endpoints = resolvePoolEndpoints(pool, podsCol)
    if len(irPool.Endpoints) == 0 {
        logger.Warn("no endpoints resolved for pool", "name", pool.Name, "namespace", pool.Namespace)
    }*/

    // Give the cluster its final name
	//out.Name = fmt.Sprintf("inference-pool-%s-%s", pool.Namespace, pool.Name)
	out.Name = in.ClusterName()

	// Standard EDS cluster config
	out.ClusterDiscoveryType = &envoyclusterv3.Cluster_Type{
		Type: envoyclusterv3.Cluster_EDS,
	}
	out.EdsClusterConfig = &envoyclusterv3.Cluster_EdsClusterConfig{
		EdsConfig: &envoycorev3.ConfigSource{
			ConfigSourceSpecifier: &envoycorev3.ConfigSource_Ads{},
			ResourceApiVersion:    envoycorev3.ApiVersion_V3,
		},
	}
	out.LbPolicy = envoyclusterv3.Cluster_ROUND_ROBIN

	// Subset config on the EPP protocol spec metadata key
	out.LbSubsetConfig = &envoyclusterv3.Cluster_LbSubsetConfig{
		SubsetSelectors: []*envoyclusterv3.Cluster_LbSubsetConfig_LbSubsetSelector{{
			Keys: []string{"x-gateway-destination-endpoint"},
		}},
		FallbackPolicy: envoyclusterv3.Cluster_LbSubsetConfig_ANY_ENDPOINT,
	}
	logger.Debug("processBackendObjectIR out", "out", out)

	// Preserve your HTTP/2 protocol options upstream
	addHTTP2(out)

	// Ignore the health value of a host when processing its removal from service discovery.
	out.IgnoreHealthOnHostRemoval = true

	// Build the KRT EndpointsForBackend and return it
	eps := ir.NewEndpointsForBackend(in)
	eps.ClusterName = out.Name
	// override the default service-cluster name so it matches
	// the subset-cluster we put into `out.Name`
	//eps.ClusterName = out.Name
	logger.Debug("processBackendObjectIR irPool.endpoints", "endpoints", irPool.Endpoints)
	for _, ep := range irPool.Endpoints {
		addr := fmt.Sprintf("%s:%d", ep.Address, ep.Port)
		// Build a mirror of the static LbEndpoint so that KRT can turn it back into a
		// ClusterLoadAssignment for EDS
		lbEp := &envoyendpointv3.LbEndpoint{
			HostIdentifier: &envoyendpointv3.LbEndpoint_Endpoint{
				Endpoint: &envoyendpointv3.Endpoint{
					Address: &envoycorev3.Address{
						Address: &envoycorev3.Address_SocketAddress{
							SocketAddress: &envoycorev3.SocketAddress{
								Address:       ep.Address,
								PortSpecifier: &envoycorev3.SocketAddress_PortValue{PortValue: uint32(ep.Port)},
							},
						},
					},
				},
			},
		}
		// The subset‐LB selector key must match the LbSubsetConfig
		eps.Add(
			ir.PodLocality{},
			ir.EndpointWithMd{
				LbEndpoint: lbEp,
				EndpointMd: ir.EndpointMetadata{
					Labels: map[string]string{
						"x-gateway-destination-endpoint": addr,
					},
				},
			},
		)
	}

	logger.Debug("processBackendObjectIR eps being returned", "eps", eps)

	return eps
}

func addHTTP2(c *envoyclusterv3.Cluster) {
    http2Opts := &upstreamsv3.HttpProtocolOptions{
        UpstreamProtocolOptions: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_{
            ExplicitHttpConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig{
                ProtocolConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
                    Http2ProtocolOptions: &envoycorev3.Http2ProtocolOptions{},
                },
            },
        },
    }
    if any, err := utils.MessageToAny(http2Opts); err == nil {
        c.TypedExtensionProtocolOptions = map[string]*anypb.Any{
            "envoy.extensions.upstreams.http.v3.HttpProtocolOptions": any,
        }
    }
}

// buildExtProcCluster builds and returns a "STRICT_DNS" cluster from the given pool.
func buildExtProcCluster(pool *inferencePool) *envoyclusterv3.Cluster {
	if pool == nil || pool.configRef == nil || len(pool.configRef.ports) != 1 {
		return nil
	}

	name := clusterNameExtProc(pool.obj.GetName(), pool.obj.GetNamespace())
	c := &envoyclusterv3.Cluster{
		Name:           name,
		ConnectTimeout: durationpb.New(10 * time.Second),
		ClusterDiscoveryType: &envoyclusterv3.Cluster_Type{
			Type: envoyclusterv3.Cluster_STRICT_DNS,
		},
		LbPolicy: envoyclusterv3.Cluster_LEAST_REQUEST,
		LoadAssignment: &envoyendpointv3.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*envoyendpointv3.LocalityLbEndpoints{{
				LbEndpoints: []*envoyendpointv3.LbEndpoint{{
					HealthStatus: envoycorev3.HealthStatus_HEALTHY,
					HostIdentifier: &envoyendpointv3.LbEndpoint_Endpoint{
						Endpoint: &envoyendpointv3.Endpoint{
							Address: &envoycorev3.Address{
								Address: &envoycorev3.Address_SocketAddress{
									SocketAddress: &envoycorev3.SocketAddress{
										Address:  fmt.Sprintf("%s.%s.svc", pool.configRef.Name, pool.obj.GetNamespace()),
										Protocol: envoycorev3.SocketAddress_TCP,
										PortSpecifier: &envoycorev3.SocketAddress_PortValue{
											PortValue: uint32(pool.configRef.ports[0].portNum),
										},
									},
								},
							},
						},
					},
				}},
			}},
		},
		// Ensure Envoy accepts untrusted certificates.
		TransportSocket: &envoycorev3.TransportSocket{
			Name: "envoy.transport_sockets.tls",
			ConfigType: &envoycorev3.TransportSocket_TypedConfig{
				TypedConfig: func() *anypb.Any {
					tlsCtx := &envoytlsv3.UpstreamTlsContext{
						CommonTlsContext: &envoytlsv3.CommonTlsContext{
							ValidationContextType: &envoytlsv3.CommonTlsContext_ValidationContext{},
						},
					}
					anyTLS, _ := utils.MessageToAny(tlsCtx)
					return anyTLS
				}(),
			},
		},
	}

	http2Opts := &upstreamsv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &upstreamsv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
					Http2ProtocolOptions: &envoycorev3.Http2ProtocolOptions{},
				},
			},
		},
	}

	anyHttp2, _ := utils.MessageToAny(http2Opts)
	c.TypedExtensionProtocolOptions = map[string]*anypb.Any{
		"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": anyHttp2,
	}

	return c
}

func clusterNameExtProc(name, ns string) string {
	return fmt.Sprintf("endpointpicker_%s_%s_ext_proc", name, ns)
}

func clusterNameOriginalDst(name, ns string) string {
	return fmt.Sprintf("endpointpicker_%s_%s_original_dst", name, ns)
}

// authorityForPool formats the gRPC authority based on the given InferencePool IR.
func authorityForPool(pool *inferencePool) string {
	ns := pool.obj.GetNamespace()
	svc := pool.configRef.Name
	port := pool.configRef.ports[0].portNum
	return fmt.Sprintf("%s.%s.svc:%d", svc, ns, port)
}

/*func makeEndpointsForPool(pool *inferencePool) *envoyendpointv3.ClusterLoadAssignment {
	clusterName := fmt.Sprintf("inference-subset-%s", pool.objMeta.GetName())
	cla := &envoyendpointv3.ClusterLoadAssignment{
		ClusterName: clusterName,
	}

	var eps []*envoyendpointv3.LbEndpoint
	for _, ep := range pool.endpoints {
		addr := fmt.Sprintf("%s:%d", ep.ip, ep.port)

		// Build a Struct {"endpoint": "<ip:port>"} for metadata
		mdStruct, err := structpb.NewStruct(map[string]interface{}{
			"x-gateway-destination-endpoint": addr,
		})
		if err != nil {
			logger.Error("failed to build endpoint metadata struct", "err", err)
			continue
		}

		lbEp := &envoyendpointv3.LbEndpoint{
			HostIdentifier: &envoyendpointv3.LbEndpoint_Endpoint{
				Endpoint: &envoyendpointv3.Endpoint{
					Address: &envoycorev3.Address{
						Address: &envoycorev3.Address_SocketAddress{
							SocketAddress: &envoycorev3.SocketAddress{
								Protocol:      envoycorev3.SocketAddress_TCP,
								Address:       ep.ip,
								PortSpecifier: &envoycorev3.SocketAddress_PortValue{PortValue: uint32(ep.port)},
							},
						},
					},
				},
			},
			// static metadata that subset‐LB will match on
			Metadata: &envoycorev3.Metadata{
				FilterMetadata: map[string]*structpb.Struct{
					// Use the same namespace your subset config expects;
					// e.g. "envoy.lb"
					"envoy.lb": mdStruct,
				},
			},
		}
		eps = append(eps, lbEp)
	}

	cla.Endpoints = []*envoyendpointv3.LocalityLbEndpoints{{
		LbEndpoints: eps,
	}}
	return cla
}*/
