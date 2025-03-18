package endpointpicker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/solo-io/go-utils/contextutils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	infextv1a2 "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"

	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/ir"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/krtcollections"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/utils/krtutil"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/wellknown"
)

const (
	// grpcPort is the default port number for a gRPC service.
	grpcPort = 9002
)

// inferencePool defines the internal representation of an inferencePool resource.
type inferencePool struct {
	objMeta metav1.ObjectMeta
	// podSelector is a label selector to select Pods that are members of the InferencePool.
	podSelector map[string]string
	// targetPort is the port number that should be targeted for Pods selected by Selector.
	targetPort int32
	// configRef is a reference to the extension configuration. A configRef is typically implemented
	// as a Kubernetes Service resource.
	configRef *service
}

// newInferencePool returns the internal representation of the given pool.
func newInferencePool(pool *infextv1a2.InferencePool) *inferencePool {
	// TODO [danehans]: Track https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/507
	// for possible changes.
	if pool == nil || pool.Spec.ExtensionRef == nil {
		return nil
	}

	port := servicePort{name: "grpc", portNum: (int32(grpcPort))}
	if pool.Spec.ExtensionRef.PortNumber != nil {
		port.portNum = int32(*pool.Spec.ExtensionRef.PortNumber)
	}

	svcIR := &service{
		ObjectSource: ir.ObjectSource{
			Group:     infextv1a2.GroupVersion.Group,
			Kind:      wellknown.InferencePoolKind,
			Namespace: pool.Namespace,
			Name:      string(pool.Spec.ExtensionRef.Name),
		},
		obj:   pool,
		ports: []servicePort{port},
	}

	return &inferencePool{
		objMeta:     pool.ObjectMeta,
		podSelector: convertSelector(pool.Spec.Selector),
		targetPort:  int32(pool.Spec.TargetPortNumber),
		configRef:   svcIR,
	}
}

// In case multiple pools attached to the same resource, we sort by creation time.
func (ir *inferencePool) CreationTime() time.Time {
	return ir.objMeta.CreationTimestamp.Time
}

func (ir *inferencePool) Selector() map[string]string {
	if ir.objMeta.Labels == nil {
		return nil
	}
	return ir.objMeta.Labels
}

func (ir *inferencePool) PodSelector() map[string]string {
	if ir.podSelector == nil {
		return nil
	}
	return ir.podSelector
}

func (ir *inferencePool) Equals(other any) bool {
	otherPool, ok := other.(*inferencePool)
	if !ok {
		return false
	}
	return maps.EqualFunc(ir.Selector(), otherPool.Selector(), func(a, b string) bool {
		return a == b
	})
}

func convertSelector(selector map[infextv1a2.LabelKey]infextv1a2.LabelValue) map[string]string {
	result := make(map[string]string, len(selector))
	for k, v := range selector {
		result[string(k)] = string(v)
	}
	return result
}

// service defines the internal representation of a Service resource.
type service struct {
	// ObjectSource is a reference to the source object. Sometimes the group and kind are not
	// populated from api-server, so set them explicitly here, and pass this around as the reference.
	ir.ObjectSource `json:",inline"`

	// obj is the original object. Opaque to us other than metadata.
	obj metav1.Object

	// ports is a list of ports exposed by the service.
	ports []servicePort
}

// servicePort is an exposed post of a service.
type servicePort struct {
	// name is the name of the port.
	name string
	// portNum is the port number used to expose the service port.
	portNum int32
}

func (s service) ResourceName() string {
	return s.ObjectSource.ResourceName()
}

func (s service) Equals(in service) bool {
	return s.ObjectSource.Equals(in.ObjectSource) && versionEquals(s.obj, in.obj)
}

var _ krt.ResourceNamer = service{}
var _ krt.Equaler[service] = service{}
var _ json.Marshaler = service{}

func (s service) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Group     string
		Kind      string
		Name      string
		Namespace string
		Ports     []servicePort
	}{
		Group:     s.Group,
		Kind:      s.Kind,
		Namespace: s.Namespace,
		Name:      s.Name,
		Ports:     s.ports,
	})
}

func versionEquals(a, b metav1.Object) bool {
	var versionEquals bool
	if a.GetGeneration() != 0 && b.GetGeneration() != 0 {
		versionEquals = a.GetGeneration() == b.GetGeneration()
	} else {
		versionEquals = a.GetResourceVersion() == b.GetResourceVersion()
	}
	return versionEquals && a.GetUID() == b.GetUID()
}

type infPoolEndpointsInputs struct {
	backendObjectIRs krt.Collection[ir.BackendObjectIR]
	pods             krt.Collection[krtcollections.LocalityPod]
	krtOpts          krtutil.KrtOptions
}


func newInfPoolEndpointsInputs(
	krtOpts krtutil.KrtOptions,
	backendObjectIRs krt.Collection[ir.BackendObjectIR],
	podCol krt.Collection[krtcollections.LocalityPod],
) *infPoolEndpointsInputs {
	return &infPoolEndpointsInputs{
		backendObjectIRs: backendObjectIRs,
		pods:             podCol,
		krtOpts:          krtOpts,
	}
}

func (i *infPoolEndpointsInputs) newInfPoolEndpoints(ctx context.Context) krt.Collection[ir.EndpointsForBackend] {
	return krt.NewCollection(i.backendObjectIRs, i.transformInfPoolEndpoints(ctx), i.krtOpts.ToOptions("InferencePoolEndpoints")...)
}

func (i *infPoolEndpointsInputs) transformInfPoolEndpoints(ctx context.Context) func(kctx krt.HandlerContext, be ir.BackendObjectIR) *ir.EndpointsForBackend {
	logger := contextutils.LoggerFrom(ctx).Desugar()

	return func(kctx krt.HandlerContext, be ir.BackendObjectIR) *ir.EndpointsForBackend {
		irPool, ok := be.ObjIr.(*inferencePool)
		if !ok || irPool == nil {
			logger.Debug("not an InferencePool object")
			return nil
		}

		logger.Debug("building endpoints for InferencePool", zap.String("pool", irPool.objMeta.Name))

		// Create a LocalityPod collection based on matching AugmentedLabels and Namespace.
		matches := krt.Fetch(kctx, i.pods, krt.FilterGeneric(func(obj any) bool {
			pod, ok := obj.(krtcollections.LocalityPod)
			if !ok {
				logger.Debug("not a LocalityPod object")
				return false
			}
			// Ensure the Pod is in the same namespace as the InferencePool IR.
			if pod.Namespace != irPool.objMeta.Namespace {
				return false
			}
			// Ensure the pod labels match the InferencePool selector
			return labelsMatch(irPool.PodSelector(), pod.AugmentedLabels)
		}))

		// Always return a valid EndpointsForBackendObjectIR instance, even if no matching pods
		ret := ir.NewEndpointsForBackend(be)

		if len(matches) == 0 {
			logger.Debug("no matching pods found for InferencePool",
			zap.String("pool", irPool.objMeta.Name),
			zap.String("namespace", irPool.objMeta.Namespace))
			return ret // Return an empty but valid EndpointsForBackendObjectIR
		}

		// Process matching Pods
		for _, pod := range matches {
			// Create Envoy LB Endpoint
			ep := krtcollections.CreateLBEndpoint(pod.IP(), uint32(irPool.targetPort), pod.AugmentedLabels, false)
			if ep.Metadata == nil {
				ep.Metadata = &corev3.Metadata{}
			}
			if ep.Metadata.FilterMetadata == nil {
				ep.Metadata.FilterMetadata = map[string]*structpb.Struct{}
			}
			logger.Debug("adding filter metadata for endpoint picker extension")
			ep.Metadata.FilterMetadata[envoySubsetNamespace] = &structpb.Struct{
				Fields: map[string]*structpb.Value{
					endpointHintKey: {Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%s:%d", pod.IP(), irPool.targetPort)}},
				},
			}

			// Add endpoint
			ret.Add(pod.Locality, ir.EndpointWithMd{
				LbEndpoint: ep,
				EndpointMd: ir.EndpointMetadata{
					Labels: pod.AugmentedLabels,
				},
			})
		}

		logger.Debug("created endpoints", zap.Int("numAddresses", len(ret.LbEps)))
		return ret
	}
}

func (i *infPoolEndpointsInputs) ResourceName() string {
	return "inference-pool-inputs"
}

// in case multiple policies attached to the same resource, we sort by policy creation time.
func (i *infPoolEndpointsInputs) CreationTime() time.Time {
	// settings always created at the same time
	return time.Time{}
}

func (i *infPoolEndpointsInputs) Equals(in any) bool {
	inputs, ok := in.(*infPoolEndpointsInputs)
	if !ok {
		return false
	}
	return i == inputs
}
