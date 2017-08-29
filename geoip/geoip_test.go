package geoip

import "testing"

func TestGeoIp(t *testing.T) {
	g, err := NewGeoMMIp("testdata/TestOldGeoIPCity.dat", "testdata/TestOldGeoIPASNum.dat")
	if err != nil {
		t.Error("Failed to load geo DB")
		t.FailNow()
	}
	anIP := "222.22.24.22"
	if "Zhengzhou" != g.GetCity(anIP) {
		t.Error("Got the wrong city!", g.GetCity(anIP))
		t.Fail()
	}
	if "CN" != g.GetCountry(anIP) {
		t.Error("Got the wrong Country!", g.GetCountry(anIP))
		t.Fail()
	}
	if "09" != g.GetRegion(anIP) {
		t.Error("Got the wrong Region!", g.GetRegion(anIP))
		t.Fail()
	}
	if "AS4538 China Education and Research Network Center" != g.GetAsn(anIP) {
		t.Error("Got the wrong ASN!", g.GetAsn(anIP))
		t.Fail()
	}
	testIP := "62.238.12.232"
	if "Terneuzen" != g.GetCity(testIP) {
		t.Error("Got the wrong city!")
		t.Fail()
	}
	if "NL" != g.GetCountry(testIP) {
		t.Error("Got the wrong Country!")
		t.Fail()
	}
	if "10" != g.GetRegion(testIP) {
		t.Error("Got the wrong Region! got: ", g.GetRegion(testIP))
		t.Fail()
	}
	if "AS15542 ZeelandNet BV" != g.GetAsn(testIP) {
		t.Error("Got the wrong ASN!", g.GetAsn(testIP))
		t.Fail()
	}

	testIP = "73.202.16.139"
	if "" != g.GetCity(testIP) {
		t.Error("Got the wrong City! got: ", g.GetRegion(testIP))
		t.Fail()
	}

	g.geoLoc = "testdata/TestNewGeoIPCity.dat"
	g.asnLoc = "testdata/TestNewGeoIPASNum.dat"
	if err = g.Reload(); err != nil {
		t.Error("Error reloading: ", err)
		t.Fail()
	}
	if "Oakland" != g.GetCity(testIP) {
		t.Error("Got the wrong City from the updated geoip db! got: ", g.GetRegion(testIP))
		t.Fail()
	}
}
