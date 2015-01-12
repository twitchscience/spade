package geoip

import (
	"fmt"

	geo "github.com/abh/geoip"
)

type GeoLookup interface {
	GetRegion(string) string
	GetCountry(string) string
	GetCity(string) string
	GetAsn(string) string
}

// Can build out a cache maybe?
type GeoMMIp struct {
	geos *geo.GeoIP
	asn  *geo.GeoIP
}

var nop GeoLookup = &NoopGeoIp{}

func LoadGeoIpDb(geoLoc string, asnLoc string) (*GeoMMIp, error) {
	c, err := geo.Open(geoLoc)
	if err != nil {
		return nil, err
	}
	asns, err := geo.Open(asnLoc)
	if err != nil {
		return nil, err
	}
	return &GeoMMIp{
		geos: c,
		asn:  asns,
	}, nil
}

func Noop() GeoLookup {
	return nop
}

func (g *GeoMMIp) GetRegion(ip string) string {
	loc := g.geos.GetRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%q", loc.Region)
}

func (g *GeoMMIp) GetCountry(ip string) string {
	loc := g.geos.GetRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%q", loc.CountryCode)
}

func (g *GeoMMIp) GetCity(ip string) string {
	loc := g.geos.GetRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%q", loc.City)
}

func (g *GeoMMIp) GetAsn(ip string) string {
	loc, _ := g.asn.GetName(ip)
	return fmt.Sprintf("%q", loc)
}

type NoopGeoIp struct{}

func (g *NoopGeoIp) GetRegion(ip string) string {
	return ""
}

func (g *NoopGeoIp) GetCountry(ip string) string {
	return ""
}

func (g *NoopGeoIp) GetAsn(ip string) string {
	return ""
}

func (g *NoopGeoIp) GetCity(ip string) string {
	return ""
}
