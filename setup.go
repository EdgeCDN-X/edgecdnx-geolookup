package edgecdnxgeolookup

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"gopkg.in/yaml.v3"
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

type LocationSpec struct {
	Name              string        `yaml:"name"`
	FallbackLocations []string      `yaml:"fallbackLocations"`
	Geolookup         GeolookupSpec `yaml:"geoLookup"`
	Nodes             []NodeSpec    `yaml:"nodes"`
}

type Location struct {
	Location LocationSpec `yaml:"location"`
}

type EdgeCDNXGeolookupRouting struct {
	FilePath  string
	Locations map[string]Location
}

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	c.Next()

	args := c.RemainingArgs()
	if len(args) != 1 {
		return plugin.Error("edgecdnxgeolookup", c.ArgErr())
	}

	routing := &EdgeCDNXGeolookupRouting{
		FilePath:  args[0],
		Locations: make(map[string]Location),
	}

	files, err := filepath.Glob(filepath.Join(routing.FilePath, "*.yaml"))
	if err != nil {
		return plugin.Error("edgecdnxgeolookup", err)
	}

	// Process each YAML file (e.g., validate or load into memory)
	for _, file := range files {
		// Example: Log the file name or perform further processing

		content, err := os.ReadFile(file)
		if err != nil {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to read file %s: %w", file, err))
		}

		var data Location
		if err := yaml.Unmarshal(content, &data); err != nil {
			log.Error(fmt.Sprintf("unmarshal error %v", err))
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("failed to parse YAML file %s: %w", file, err))
		}

		if _, ok := routing.Locations[data.Location.Name]; ok {
			return plugin.Error("edgecdnxgeolookup", fmt.Errorf("duplicate location name %s in file %s", data.Location.Name, file))
		}

		routing.Locations[data.Location.Name] = data
	}

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return EdgeCDNXGeolookup{Next: next, Routing: routing}
	})

	// All OK, return a nil error.
	return nil
}
