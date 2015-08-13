package geoip

import "testing"

func TestGeoIp(t *testing.T) {
	g, err := LoadGeoIpDb("../build/config/GeoLiteCity.dat", "../build/config/GeoIPASNum.dat")
	if err != nil {
		t.Error("Failed to load geo DB")
		t.FailNow()
	}
	anIp := "222.22.24.22"
	if "\"Zhengzhou\"" != g.GetCity(anIp) {
		t.Error("Got the wrong city!", g.GetCity(anIp))
		t.Fail()
	}
	if "\"CN\"" != g.GetCountry(anIp) {
		t.Error("Got the wrong Country!", g.GetCountry(anIp))
		t.Fail()
	}
	if "\"09\"" != g.GetRegion(anIp) {
		t.Error("Got the wrong Region!", g.GetRegion(anIp))
		t.Fail()
	}
	if "\"AS4538 China Education and Research Network Center\"" != g.GetAsn(anIp) {
		t.Error("Got the wrong ASN!", g.GetAsn(anIp))
		t.Fail()
	}
	testIp := "62.238.12.232"
	if "\"Terneuzen\"" != g.GetCity(testIp) {
		t.Error("Got the wrong city!")
		t.Fail()
	}
	if "\"NL\"" != g.GetCountry(testIp) {
		t.Error("Got the wrong Country!")
		t.Fail()
	}
	if "\"10\"" != g.GetRegion(testIp) {
		t.Error("Got the wrong Region! got: ", g.GetRegion(testIp))
		t.Fail()
	}
	if "\"AS15542 ZeelandNet BV\"" != g.GetAsn(testIp) {
		t.Error("Got the wrong ASN!", g.GetAsn(testIp))
		t.Fail()
	}

}
