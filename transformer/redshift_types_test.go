package transformer

import (
	"encoding/json"
	"testing"
)

func _type_runner(t *testing.T, input interface{}, _type RedshiftType,
	expected string, shouldFail bool) {
	actual, err := _type.Transformer(input)
	if err != nil && !shouldFail {
		t.Log(err)
		t.Fail()
	} else if err != nil {
		return
	}
	if expected != actual {
		t.Logf("Expected %v got %v\n", expected, actual)
		t.Fail()
	}
}

func TestIntConversion(t *testing.T) {
	normalInteger := RedshiftType{intFormat(64), "_"}
	_type_runner(t, json.Number("42"), normalInteger, "42", false)
	_type_runner(t, "42", normalInteger, "42", false)

	// These should all fail as they are incorrect representations.
	// In the future we will want to make this less strict
	_type_runner(t, json.Number("42.2"), normalInteger, "", true)
	_type_runner(t, nil, normalInteger, "", true)
	_type_runner(t, "hashs", normalInteger, "", true)
	_type_runner(t, "12hs", normalInteger, "", true)
}

func TestSmallIntConversion(t *testing.T) {
	smallInteger := RedshiftType{intFormat(8), "_"}
	_type_runner(t, json.Number("128"), smallInteger, "", true)
	_type_runner(t, "127", smallInteger, "127", false)
}

func TestFloatConversion(t *testing.T) {
	normalFloat := RedshiftType{floatFormat, "_"}
	_type_runner(t, json.Number("1.234"), normalFloat, "1.234", false)
	_type_runner(t, json.Number("1234.0"), normalFloat, "1234", false)
	_type_runner(t, json.Number("1234"), normalFloat, "1234", false)
	_type_runner(t, "1234.0", normalFloat, "1234", false)
	_type_runner(t, "1234.0000000000000000000000000000000000000000000000000000000000001",
		normalFloat, "1234", false)
	_type_runner(t, nil, normalFloat, "", true)

}

func TestVarCharConversion(t *testing.T) {
	normalVarChar := RedshiftType{varcharFormat, "_"}
	_type_runner(t, "tests", normalVarChar, "\"tests\"", false)
	_type_runner(t, "test\n", normalVarChar, "\"test\\n\"", false)
	_type_runner(t, "test\t", normalVarChar, "\"test\\t\"", false)
	// This will pass as we are relaxing the strict len check here as RS ingest can handle it
	_type_runner(t, "testss", normalVarChar, "\"testss\"", false)
	_type_runner(t, json.Number("1234.0"), normalVarChar, "", true)
}

func TestTimestampConversion(t *testing.T) {
	unixDateTime := RedshiftType{unixTimeFormat, "_"}
	_type_runner(t, json.Number("1382033155.045"), unixDateTime, "2013-10-17 11:05:55.045", false)
	_type_runner(t, json.Number("1382033155"), unixDateTime, "2013-10-17 11:05:55", false)
	_type_runner(t, json.Number("138203315"), unixDateTime, "", true)
	_type_runner(t, "asd", unixDateTime, "", true)

	otherDateTime := RedshiftType{genTimeFormat("2006-01-02 15:04:05"), "_"}
	_type_runner(t, "2013-10-17 11:05:55", otherDateTime, "2013-10-17 11:05:55", false)
	_type_runner(t, "2013-10-17 105:55", otherDateTime, "", true)
}

func TestBooleanConversion(t *testing.T) {
	booleanConverter := RedshiftType{boolFormat, "_"}
	_type_runner(t, true, booleanConverter, "true", false)
	_type_runner(t, "true", booleanConverter, "", true)
	_type_runner(t, "asd", booleanConverter, "", true)
}

func TestIpConversion(t *testing.T) {
	err := SetGeoDB("../build/config/GeoLiteCity.dat", "../build/config/GeoIPASNum.dat")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	ipCityConverter := RedshiftType{ipCityFormat, "_"}
	_type_runner(t, "222.22.24.22", ipCityConverter, "\"Zhengzhou\"", false)

	ipCountryConverter := RedshiftType{ipCountryFormat, "_"}
	_type_runner(t, "222.22.24.22", ipCountryConverter, "\"CN\"", false)

	ipRegionConverter := RedshiftType{ipRegionFormat, "_"}
	_type_runner(t, "222.22.24.22", ipRegionConverter, "\"09\"", false)

	ipAsnConverter := RedshiftType{ipAsnFormat, "_"}
	_type_runner(t, "222.22.24.22", ipAsnConverter, "\"AS4538 China Education and Research Network Center\"", false)

	ipAsnIntConverter := RedshiftType{ipAsnIntFormat, "_"}
	_type_runner(t, "222.22.24.22", ipAsnIntConverter, "4538", false)
}

func TestHashTransformer(t *testing.T) {
	hashTransformerConverter := RedshiftType{hashTransformer, "_"}
	_type_runner(t, "asd", hashTransformerConverter, "2014669166", false)
}
