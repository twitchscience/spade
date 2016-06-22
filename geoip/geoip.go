package geoip

import (
	"fmt"
	"sync"

	geo "github.com/abh/geoip"
)

type GeoLookup interface {
	GetRegion(string) string
	GetCountry(string) string
	GetCity(string) string
	GetAsn(string) string
	Reload() error
}

// Can build out a cache maybe?
type GeoMMIp struct {
	geos   *geo.GeoIP
	asn    *geo.GeoIP
	geoLoc string
	asnLoc string
	sync.RWMutex
}

var nop GeoLookup = &NoopGeoIp{}

func NewGeoMMIp(geoLoc string, asnLoc string) (*GeoMMIp, error) {
	g := GeoMMIp{
		geoLoc: geoLoc,
		asnLoc: asnLoc,
	}
	err := g.Reload()
	return &g, err
}

func Noop() GeoLookup {
	return nop
}

func (g *GeoMMIp) Reload() error {
	g.Lock()
	defer g.Unlock()

	c, err := geo.Open(g.geoLoc)
	if err != nil {
		return err
	}
	asns, err := geo.Open(g.asnLoc)
	if err != nil {
		return err
	}
	g.geos = c
	g.asn = asns
	return nil
}

func (g *GeoMMIp) getGeosRecord(ip string) *geo.GeoIPRecord {
	g.RLock()
	loc := g.geos.GetRecord(ip)
	g.RUnlock()
	return loc

}

func (g *GeoMMIp) GetRegion(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%s", loc.Region)
}

func (g *GeoMMIp) GetCountry(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%s", loc.CountryCode)
}

func (g *GeoMMIp) GetCity(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return fmt.Sprintf("%s", loc.City)
}

func (g *GeoMMIp) GetAsn(ip string) string {
	g.RLock()
	loc, _ := g.asn.GetName(ip)
	g.RUnlock()
	return fmt.Sprintf("%s", loc)
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

func (g *NoopGeoIp) Reload() error {
	return nil
}
