// Package example is a CoreDNS plugin that prints "example" to stdout on every packet received.
//
// It serves as an example CoreDNS plugin with numerous code comments.
package edgecdnxgeolookup

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"slices"
	"sync"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/metadata"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/miekg/dns"
)

// Example is an example plugin to show how to write a plugin.
type EdgeCDNXGeolookup struct {
	Next              plugin.Handler
	Locations         map[string]infrastructurev1alpha1.Location
	Sync              *sync.RWMutex
	InformerSynced    func() bool
	ConsulClient      *consulapi.Client
	ConsulHealthCache *Cache[bool]
	Ttl               uint32
}

type EdgeCDNXGeolookupResponseWriter struct {
}

func (e EdgeCDNXGeolookup) IsPrefixRouted(ctx context.Context) (string, error) {
	if prefixFunc := metadata.ValueFunc(ctx, "edgecdnxprefixlist/location"); prefixFunc != nil {
		if staticLocation := prefixFunc(); staticLocation != "" {
			return staticLocation, nil
		}
		return "", fmt.Errorf("No static route found")
	}
	return "", fmt.Errorf("Prefixlist metadata module not initialized")
}

func (e EdgeCDNXGeolookup) GetServiceCache(ctx context.Context) (string, error) {
	if cacheFunc := metadata.ValueFunc(ctx, "edgecdnxservices/cache"); cacheFunc != nil {
		if serviceCache := cacheFunc(); serviceCache != "" {
			return serviceCache, nil
		}
		return "", fmt.Errorf("No service cache found")
	}
	return "", fmt.Errorf("Service cache metadata module not initialized")
}

func (e EdgeCDNXGeolookup) GetCustomer(ctx context.Context) (string, error) {
	if customerFunc := metadata.ValueFunc(ctx, "edgecdnxservices/customer"); customerFunc != nil {
		if customer := customerFunc(); customer != "" {
			return customer, nil
		}
		return "", fmt.Errorf("Customer not found in meta")
	}
	return "", fmt.Errorf("Service customer metadata module not initialized")
}

func (e EdgeCDNXGeolookup) PerformGeoLookup(ctx context.Context, cache string) (string, error) {
	maxValue := 0
	locationScore := make(map[string]int)

	for locationName, location := range e.Locations {
		if slices.IndexFunc(location.Spec.NodeGroups, func(ng infrastructurev1alpha1.NodeGroupSpec) bool { return ng.Name == cache }) == -1 {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: skipping location %s as it does not have node group for cache %s", locationName, cache))
			continue
		}

		for attrName, attribute := range location.Spec.GeoLookup.Attributes {
			if lookupFunc := metadata.ValueFunc(ctx, attrName); lookupFunc != nil {
				if lookupValue := lookupFunc(); lookupValue != "" {
					for _, attributeValue := range attribute.Values {
						if attributeValue.Value == lookupValue {
							log.Debug(fmt.Sprintf("edgecdnxgeolookup: found attribute %s with value %s", attrName, lookupValue))

							currScore, ok := locationScore[locationName]
							if !ok {
								currScore = 0
							}
							if currScore+attribute.Weight > maxValue {
								maxValue = currScore + attribute.Weight + attributeValue.Weight
							}
							locationScore[locationName] = currScore + attribute.Weight + attributeValue.Weight
						}
					}
				}
			}
		}
	}

	winners := make([]string, 0)

	for locationName, score := range locationScore {
		if score == maxValue {
			winners = append(winners, locationName)
		}
	}

	log.Debug(fmt.Sprintf("edgecdnxgeolookup: found %d locations with score %d: %v", len(winners), maxValue, winners))

	if len(winners) > 1 {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: multiple locations found with same score %d: %v", maxValue, winners))

		randomNumber := rand.Float64()
		totalWeigth := 0

		for _, locationName := range winners {
			location := e.Locations[locationName]
			totalWeigth = totalWeigth + location.Spec.GeoLookup.Weight
		}

		selector := (float64(totalWeigth) * randomNumber)

		currentWeight := 0
		for _, locationName := range winners {
			location := e.Locations[locationName]
			currentWeight += location.Spec.GeoLookup.Weight
			if int(selector) <= currentWeight {
				return locationName, nil
			}
		}
	}

	if len(winners) == 1 {
		return winners[0], nil
	}

	return "", errors.New("No Location Found")
}

func (e EdgeCDNXGeolookup) ApplyHash(location *infrastructurev1alpha1.Location, hashInput string, filters struct {
	Cache string
}) (infrastructurev1alpha1.NodeSpec, error) {
	filteredNodes := make([]infrastructurev1alpha1.NodeSpec, 0)

	if location.Spec.MaintenanceMode {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location %s is in maintenance mode", location.Name))
		return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("Location %s is in maintenance mode", location.Name)
	}

	// Add only nodes which are not in maintenance mode and match the cache filter
	for _, ng := range location.Spec.NodeGroups {
		if ng.Name == filters.Cache {
			for _, node := range ng.Nodes {
				if node.MaintenanceMode {
					continue
				}
				filteredNodes = append(filteredNodes, node)
			}
		}
	}

	for {
		if len(filteredNodes) == 0 {
			return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("No healthy nodes found in location %s with cache %s", location.Name, filters.Cache)
		}

		hash := md5.Sum([]byte(hashInput))
		lastFourBytes := hash[len(hash)-4:]
		hashValue := uint32(lastFourBytes[0])<<24 | uint32(lastFourBytes[1])<<16 | uint32(lastFourBytes[2])<<8 | uint32(lastFourBytes[3])
		nodeIndex := int(hashValue % uint32(len(filteredNodes)))

		nodeName := fmt.Sprintf("%s.%s.edgecdnx.com", filteredNodes[nodeIndex].Name, location.Name)

		cacheService := fmt.Sprintf("cache-%s", nodeName)
		healthy, ok := e.ConsulHealthCache.Get(cacheService)

		if !ok {
			health, _, err := e.ConsulClient.Health().Service(cacheService, "", false, &consulapi.QueryOptions{
				UseCache:   true,
				AllowStale: true,
			})

			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Error fetching health for node %s - %v", nodeName, err))
			}

			healthy = true
			for _, check := range health {
				if check.Checks.AggregatedStatus() != consulapi.HealthPassing {
					log.Debugf("edgecdnxgeolookup: Node %s is not healthy, trying next node", nodeName)
					filteredNodes = slices.Delete(filteredNodes, nodeIndex, nodeIndex+1)
					healthy = false
				}
			}
			log.Debugf("edgecdnxgeolookup: cache miss: %s - %v", cacheService, healthy)
			e.ConsulHealthCache.Set(cacheService, healthy)
		} else {
			log.Debugf("edgecdnxgeolookup: cache hit: %s - %v", cacheService, healthy)
		}

		if healthy {
			log.Debugf("edgecdnxgeolookup: Node %s is healthy, returning node", nodeName)
			return filteredNodes[nodeIndex], nil
		}
	}
}

// ServeDNS implements the plugin.Handler interface. This method gets called when example is used
// in a Server.
func (e EdgeCDNXGeolookup) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	cache, err := e.GetServiceCache(ctx)
	if err != nil {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Cache not found - %v", err))
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	locationName, err := e.IsPrefixRouted(ctx)
	if err != nil {
		locationName, err = e.PerformGeoLookup(ctx, cache)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}
	}

	e.Sync.RLock()
	defer e.Sync.RUnlock()

	location, ok := e.Locations[locationName]
	if !ok {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	log.Debug(fmt.Sprintf("edgecdnxgeolookup: Routing to location: %s\n", location.Name))

	node, err := e.ApplyHash(&location, state.Name(), struct{ Cache string }{cache})
	if err != nil {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Hashing error - %v", err))

		for _, fbLoc := range location.Spec.FallbackLocations {
			fbLocation, ok := e.Locations[fbLoc]
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Falling back to location %s", fbLoc))
			if !ok {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback location %s not found", fbLoc))
				continue
			}

			node, err = e.ApplyHash(&fbLocation, state.Name(), struct{ Cache string }{cache})
			if err == nil {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s successful", fbLoc))
				break
			}
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s failed - %v", fbLoc, err))
		}

		if err != nil {
			log.Error(fmt.Sprintf("edgecdnxgeolookup: No nodes found for request %s - %v", state.Name(), err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	srcIP := net.ParseIP(state.IP())
	if o := state.Req.IsEdns0(); o != nil {
		for _, s := range o.Option {
			if e, ok := s.(*dns.EDNS0_SUBNET); ok {
				srcIP = e.Address
				break
			}
		}
	}

	log.Debug(fmt.Sprintf("edgecdnxgeolookup: srcIP %s", srcIP))

	if srcIP.To4() != nil {
		res := new(dns.A)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: e.Ttl}
		parsed := net.ParseIP(node.Ipv4)
		res.A = parsed
		m.Answer = append(m.Answer, res)
	} else {
		res := new(dns.AAAA)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: e.Ttl}
		parsed := net.ParseIP(node.Ipv6)
		res.AAAA = parsed
		m.Answer = append(m.Answer, res)
	}

	state.SizeAndDo(m)
	m = state.Scrub(m)
	err = w.WriteMsg(m)

	if err != nil {
		log.Error(fmt.Sprintf("edgecdnxgeolookup: DNS response write failure %v", err))
		return dns.RcodeServerFailure, err
	}
	log.Info(fmt.Sprintf("edgecdnxgeolookup: DNS response %s %v", state.Name(), m.Answer))
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (e EdgeCDNXGeolookup) Name() string { return "edgecdnxgeolookup" }

// ResponsePrinter wrap a dns.ResponseWriter and will write example to standard output when WriteMsg is called.
type ResponsePrinter struct {
	dns.ResponseWriter
}

// NewResponsePrinter returns ResponseWriter.
func NewResponsePrinter(w dns.ResponseWriter) *ResponsePrinter {
	return &ResponsePrinter{ResponseWriter: w}
}

// WriteMsg calls the underlying ResponseWriter's WriteMsg method and prints "example" to standard output.
func (r *ResponsePrinter) WriteMsg(res *dns.Msg) error {
	return r.ResponseWriter.WriteMsg(res)
}
