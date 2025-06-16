// Package example is a CoreDNS plugin that prints "example" to stdout on every packet received.
//
// It serves as an example CoreDNS plugin with numerous code comments.
package edgecdnxgeolookup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"

	"github.com/coredns/coredns/plugin/metadata"

	"crypto/md5"

	"slices"

	"github.com/miekg/dns"
)

// Example is an example plugin to show how to write a plugin.
type EdgeCDNXGeolookup struct {
	Next    plugin.Handler
	Routing *EdgeCDNXGeolookupRouting
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

func (e EdgeCDNXGeolookup) PerformGeoLookup(ctx context.Context) (string, error) {
	maxValue := 0
	locationScore := make(map[string]int)

	for locationName, location := range e.Routing.Locations {
		for attrName, attribute := range location.Geolookup.Attributes {
			if lookupFunc := metadata.ValueFunc(ctx, attrName); lookupFunc != nil {
				if lookupValue := lookupFunc(); lookupValue != "" {
					log.Debug(fmt.Sprintf("found attribute %s with value %s", attrName, lookupValue))

					if slices.ContainsFunc(attribute.Values, func(attr GeolookupAtributeValueSpec) bool {
						return attr.Value == lookupValue
					}) {
						currScore, ok := locationScore[locationName]
						if !ok {
							currScore = 0
						}
						if currScore+attribute.Weight > maxValue {
							maxValue = currScore + attribute.Weight
						}
						locationScore[locationName] = currScore + attribute.Weight
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
			location := e.Routing.Locations[locationName]
			totalWeigth = totalWeigth + location.Geolookup.Weight
		}

		selector := (float64(totalWeigth) * randomNumber)

		currentWeight := 0
		for _, locationName := range winners {
			location := e.Routing.Locations[locationName]
			currentWeight += location.Geolookup.Weight
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

func (e EdgeCDNXGeolookup) ApplyHash(location *Location, hashInput string) (*NodeSpec, error) {
	if len(location.Nodes) == 0 {
		return nil, fmt.Errorf("No nodes found in location %s", location.Name)
	}
	hash := md5.Sum([]byte(hashInput))

	lastFourBytes := hash[len(hash)-4:]
	hashValue := uint32(lastFourBytes[0])<<24 | uint32(lastFourBytes[1])<<16 | uint32(lastFourBytes[2])<<8 | uint32(lastFourBytes[3])
	nodeIndex := int(hashValue % uint32(len(location.Nodes)))

	return &location.Nodes[nodeIndex], nil
}

// ServeDNS implements the plugin.Handler interface. This method gets called when example is used
// in a Server.
func (e EdgeCDNXGeolookup) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	locationName, err := e.IsPrefixRouted(ctx)
	if err != nil {
		locationName, err = e.PerformGeoLookup(ctx)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}
	}

	location, ok := e.Routing.Locations[locationName]
	if !ok {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}

	log.Debug(fmt.Sprintf("Routing to location: %s\n", location.Name))

	node, err := e.ApplyHash(&location, state.Name())
	if err != nil {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Hashing error - %v", err))
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
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
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 180}
		parsed := net.ParseIP(node.Ipv4)
		res.A = parsed
		m.Answer = append(m.Answer, res)
	} else {
		res := new(dns.AAAA)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 180}
		parsed := net.ParseIP(node.Ipv6)
		res.AAAA = parsed
		m.Answer = append(m.Answer, res)
	}

	ok = state.SizeAndDo(m)
	if !ok {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: SizeAndDo error - %v", err))
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
	}
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
