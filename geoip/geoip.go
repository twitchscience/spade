package geoip

import (
	"sync"

	geo "github.com/abh/geoip"
)

// GeoLookup defines functions to get properties of an IP address and Reload databases.
type GeoLookup interface {
	GetRegion(string) string
	GetCountry(string) string
	GetCity(string) string
	GetAsn(string) string
	Reload() error
}

// GeoMMIP is a GeoLookup backed by MaxMind.
// Can build out a cache maybe?
type GeoMMIP struct {
	geos   *geo.GeoIP
	asn    *geo.GeoIP
	geoLoc string
	asnLoc string
	sync.RWMutex
}

// NewGeoMMIp returns a GeoMMIP using the given geo and asn database locations.
func NewGeoMMIp(geoLoc string, asnLoc string) (*GeoMMIP, error) {
	g := GeoMMIP{
		geoLoc: geoLoc,
		asnLoc: asnLoc,
	}
	err := g.Reload()
	return &g, err
}

// Reload reloads the configured databases.
func (g *GeoMMIP) Reload() error {
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

func (g *GeoMMIP) getGeosRecord(ip string) *geo.GeoIPRecord {
	g.RLock()
	defer g.RUnlock()
	return g.geos.GetRecord(ip)
}

// GetRegion returns the region associated with the ip.
func (g *GeoMMIP) GetRegion(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return loc.Region
}

// GetCountry returns the country associated with the ip.
func (g *GeoMMIP) GetCountry(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return loc.CountryCode
}

// GetCity returns the city associated with the ip.
func (g *GeoMMIP) GetCity(ip string) string {
	loc := g.getGeosRecord(ip)
	if loc == nil {
		return ""
	}
	return loc.City
}

// GetAsn returns the ASN associated with the ip.
func (g *GeoMMIP) GetAsn(ip string) string {
	g.RLock()
	defer g.RUnlock()
	loc, _ := g.asn.GetName(ip)
	return loc
}

// NoopGeoIP is a GeoLookup that always returns empty strings.
type NoopGeoIP struct{}

var nop GeoLookup = &NoopGeoIP{}

// Noop returns a GeoLookup that always returns empty strings.
func Noop() GeoLookup {
	return nop
}

// GetRegion returns an empty string.
func (g *NoopGeoIP) GetRegion(ip string) string {
	return ""
}

// GetCountry returns an empty string.
func (g *NoopGeoIP) GetCountry(ip string) string {
	return ""
}

// GetAsn returns an empty string.
func (g *NoopGeoIP) GetAsn(ip string) string {
	return ""
}

// GetCity returns an empty string.
func (g *NoopGeoIP) GetCity(ip string) string {
	return ""
}

// Reload does nothing.
func (g *NoopGeoIP) Reload() error {
	return nil
}
