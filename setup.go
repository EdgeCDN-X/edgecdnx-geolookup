package edgecdnxgeolookup

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"

	"k8s.io/apimachinery/pkg/runtime"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientsetscheme "k8s.io/client-go/kubernetes/scheme"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
)

// init registers this plugin.
func init() { plugin.Register("edgecdnxgeolookup", setup) }

type NodeSpec struct {
	Name   string   `yaml:"name"`
	Caches []string `yaml:"caches"`
	Ipv4   string   `yaml:"ipv4"`
	Ipv6   string   `yaml:"ipv6,omitempty"`
}

type GeolookupAtributeValueSpec struct {
	Value  string `yaml:"value"`
	Weight int    `yaml:"weight,omitempty"`
}

type GeoLookupAttributeSpec struct {
	Weight int                          `yaml:"weight"`
	Values []GeolookupAtributeValueSpec `yaml:"values"`
}

type GeolookupSpec struct {
	Weight     int                               `yaml:"weight"`
	Attributes map[string]GeoLookupAttributeSpec `yaml:"attributes"`
}

type Location struct {
	Name              string        `yaml:"name"`
	FallbackLocations []string      `yaml:"fallbackLocations"`
	Geolookup         GeolookupSpec `yaml:"geoLookup"`
	Nodes             []NodeSpec    `yaml:"nodes"`
}

type EdgeCDNXGeolookupRouting struct {
	Namespace string
	Locations map[string]Location
}

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	scheme := runtime.NewScheme()
	clientsetscheme.AddToScheme(scheme)
	infrastructurev1alpha1.AddToScheme(scheme)

	kubeconfig := ctrl.GetConfigOrDie()
	kubeclient, err := client.New(kubeconfig, client.Options{Scheme: scheme})
	if err != nil {
		return plugin.Error("edgecdnxprefixlist", fmt.Errorf("failed to create Kubernetes client: %w", err))
	}

	c.Next()

	args := c.RemainingArgs()
	if len(args) != 1 {
		return plugin.Error("edgecdnxgeolookup", c.ArgErr())
	}

	routing := &EdgeCDNXGeolookupRouting{
		Namespace: args[0],
		Locations: make(map[string]Location),
	}

	locations := &infrastructurev1alpha1.LocationList{}
	if err := kubeclient.List(context.TODO(), locations, &client.ListOptions{
		Namespace: routing.Namespace,
	}); err != nil {
		return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to list locations: %w", err))
	}

	for _, location := range locations.Items {
		loc := &Location{}
		loc.Name = location.Name
		loc.FallbackLocations = location.Spec.FallbackLocations

		geolookup, err := json.Marshal(location.Spec.GeoLookup)
		if err != nil {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to marshal geolookup spec: %w", err))
		}
		err = json.Unmarshal(geolookup, &loc.Geolookup)
		if err != nil {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to unmarshal geolookup spec: %w", err))
		}

		nodes, err := json.Marshal(location.Spec.Nodes)
		if err != nil {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to marshal nodes spec: %w", err))
		}
		err = json.Unmarshal(nodes, &loc.Nodes)
		if err != nil {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to unmarshal geolookup spec: %w", err))
		}

		routing.Locations[location.Name] = *loc
	}

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return EdgeCDNXGeolookup{Next: next, Routing: routing}
	})

	// All OK, return a nil error.
	return nil
}
