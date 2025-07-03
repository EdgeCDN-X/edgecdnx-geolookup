package edgecdnxgeolookup

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	consulapi "github.com/hashicorp/consul/api"
)

// init registers this plugin.
func init() { plugin.Register("edgecdnxgeolookup", setup) }

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	scheme := kruntime.NewScheme()
	clientsetscheme.AddToScheme(scheme)
	infrastructurev1alpha1.AddToScheme(scheme)

	kubeconfig := ctrl.GetConfigOrDie()
	c.Next() // plugin name

	var namespace, consulEndpoint string
	consulcachettl := time.Second * 30
	consulcachecleanupinterval := time.Minute * 1
	locations := make(map[string]infrastructurev1alpha1.Location)

	for c.NextBlock() {
		val := c.Val()
		args := c.RemainingArgs()
		if val == "namespace" {
			namespace = args[0]
		}
		if val == "consulEndpoint" {
			consulEndpoint = args[0]
		}
		if val == "consulcachettl" {
			persedttl, err := time.ParseDuration(args[0])
			if err != nil {
				return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to parse consulcachettl: %w", err))
			}
			consulcachettl = persedttl
		}
		if val == "consulcachecleanupinterval" {
			parsedcleanup, err := time.ParseDuration(args[0])
			if err != nil {
				return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to parse consulcachecleanupinterval: %w", err))
			}
			consulcachecleanupinterval = parsedcleanup
		}
	}

	clientSet, err := dynamic.NewForConfig(kubeconfig)
	if err != nil {
		return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to create dynamic client: %w", err))
	}

	fac := dynamicinformer.NewFilteredDynamicSharedInformerFactory(clientSet, 10*time.Minute, namespace, nil)
	informer := fac.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.GroupVersion.Group,
		Version:  infrastructurev1alpha1.GroupVersion.Version,
		Resource: "locations",
	}).Informer()

	log.Infof("edgecdnxgeolookup: Starting informer for locations in namespace %s", namespace)

	sem := &sync.RWMutex{}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			l_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: Failed to cast object to unstructured.Unstructured")
				return
			}

			temp, err := json.Marshal(l_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: Failed to marshal location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: Failed to unmarshal location object: %v", err)
				return
			}

			sem.Lock()
			defer sem.Unlock()
			locations[location.Name] = *location
			log.Infof("edgecdnxgeolookup: Added Location %s", location.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: expected Location object, got %T", s_new_raw)
				return
			}
			temp, err := json.Marshal(s_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to unmarshal Location object: %v", err)
				return
			}
			sem.Lock()
			defer sem.Unlock()
			loc, exists := locations[location.Name]
			if !exists {
				log.Errorf("edgecdnxgeolookup: Location %s not found in routing map", location.Name)
				return
			}
			locations[location.Name] = loc
			log.Infof("edgecdnxgeolookup: Updated Location %s", location.Name)
		},
		DeleteFunc: func(obj any) {
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: expected Location object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to unmarshal Location object: %v", err)
				return
			}

			sem.Lock()
			defer sem.Unlock()
			delete(locations, location.Name)
			log.Infof("edgecdnxgeolookup: Deleted Location %s", location.Name)
		},
	})

	factoryCloseChan := make(chan struct{})
	fac.Start(factoryCloseChan)

	consulCache := NewCache[bool](consulcachettl, consulcachecleanupinterval)

	c.OnShutdown(func() error {
		log.Infof("edgecdnxgeolookup: Shutting down informer")
		close(factoryCloseChan)
		fac.Shutdown()
		consulCache.Stop()
		return nil
	})

	consulConfig := consulapi.DefaultConfig()
	consulConfig.Address = consulEndpoint
	consulClient, err := consulapi.NewClient(consulConfig)

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return EdgeCDNXGeolookup{Next: next,
			Locations:         locations,
			Sync:              sem,
			InformerSynced:    informer.HasSynced,
			ConsulClient:      consulClient,
			ConsulHealthCache: consulCache,
		}
	})

	// All OK, return a nil error.
	return nil
}
