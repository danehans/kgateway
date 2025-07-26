package endpointpicker

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"istio.io/istio/pkg/kube/krt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	infextv1a2 "sigs.k8s.io/gateway-api-inference-extension/api/v1alpha2"

	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/ir"
	"github.com/kgateway-dev/kgateway/v2/internal/kgateway/wellknown"
)

const (
	// grpcPort is the default port number for a gRPC service.
	grpcPort = 9002
)

// inferencePool defines the internal representation of an inferencePool resource.
type inferencePool struct {
    // obj is the original object. Opaque to us other than metadata.
    obj metav1.Object
	// podSelector is a label selector to select Pods that are members of the InferencePool.
	podSelector map[string]string
	// targetPort is the port number that should be targeted for Pods selected by Selector.
	targetPort int32
	// configRef is a reference to the extension configuration. A configRef is typically implemented
	// as a Kubernetes Service resource.
	configRef *service
	// mu is a mutex to protect access to the errors list.
	mu sync.Mutex
	// errors is a list of errors that occurred while processing the InferencePool.
	errors []error
	// Endpoints define the list of endpoints resolved by the podSelector.
	Endpoints Endpoints
}

// newInferencePool returns the internal representation of the given pool.
func newInferencePool(pool *infextv1a2.InferencePool, eps []Endpoint) *inferencePool {
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
		obj:         pool,
		podSelector: convertSelector(pool.Spec.Selector),
		targetPort:  int32(pool.Spec.TargetPortNumber),
		configRef:   svcIR,
		Endpoints:   eps,
	}
}

// In case multiple pools attached to the same resource, we sort by creation time.
func (ir *inferencePool) CreationTime() time.Time {
	return ir.obj.GetCreationTimestamp().Time
}

func (ir *inferencePool) Selector() map[string]string {
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

// setErrors atomically replaces p.errors under lock.
func (p *inferencePool) setErrors(errs []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errors = errs
}

// snapshotErrors returns a copy of p.errors under lock.
func (p *inferencePool) snapshotErrors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]error, len(p.errors))
	copy(out, p.errors)
	return out
}

// hasErrors checks if the inferencePool has any errors.
func (p *inferencePool) hasErrors() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.errors) > 0
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

// Endpoint defines the internal representation of an Endpoint.
type Endpoint struct {
	// Address is the IP address address of the endpoint.
	Address string
	// Port is the port exposed by the endpoint.
	Port int32
}

// Endpoints is a named slice of Endpoint.
type Endpoints []Endpoint

// String satisfies fmt.Stringer on a single Endpoint.
func (e Endpoint) String() string {
    return fmt.Sprintf("%s:%d", e.Address, e.Port)
}

// ToString returns the comma-joined list of endpoints, e.g. "10.0.0.1:9000,10.0.0.2:9000".
func (eps Endpoints) ToString() string {
    if len(eps) == 0 {
        return ""
    }

    parts := make([]string, len(eps))
    for i, ep := range eps {
        parts[i] = ep.String()
    }

    return strings.Join(parts, ",")
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
