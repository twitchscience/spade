package transformer

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twitchscience/spade/geoip"
)

// Contains transformers to cast and munge properties coming in into types
// Consistent whith the incoming schemas.
//
// There are two types of transformers: Vanilla transformers, and transform generators.
// Transform generators are column transformer that require input from the
// config to determine how they parse things. The quintessential use case for this is
// for time transformers. Transform generators allow the user to define how
// the transformer should parse a inbound property.

// All times are in PST.
var PST = getPST()

func getPST() *time.Location {
	PST, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	return PST
}

// A RedshiftType combines a way to get the input to the ColumnTransformer.
// Basically it performs Transformer(Event[EventProperty]) -> Column.
type RedshiftType struct {
	Transformer  ColumnTranformer
	InboundName  string
	OutboundName string
}

func (r *RedshiftType) Format(eventProperties map[string]interface{}) (string, string, error) {
	if p, ok := eventProperties[r.InboundName]; ok {
		value, err := r.Transformer(p)
		return r.OutboundName, value, err
	}
	return "", "", ColumnNotFoundError
}

// Returns us a Transformer for a given string
func GetTransform(t_type string) ColumnTranformer {
	if t, ok := transformMap[t_type]; ok {
		return t
	}
	if t_type[0] == 'f' { // were building a transform function
		transformParams := strings.Split(t_type, "@")
		if len(transformParams) < 3 {
			return nil
		}
		if transformGenerator, ok := transformGeneratorMap[transformParams[1]]; ok {
			return transformGenerator(transformParams[2])
		}
		return nil
	}
	return nil
}

// New types should register here
var (
	transformMap = map[string]ColumnTranformer{
		"int":                intFormat(32),
		"bigint":             intFormat(64),
		"float":              floatFormat,
		"varchar":            varcharFormat,
		"bool":               boolFormat,
		"ipCity":             ipCityFormat,
		"ipCountry":          ipCountryFormat,
		"ipRegion":           ipRegionFormat,
		"ipAsn":              ipAsnFormat,
		"ipAsnInteger":       ipAsnIntFormat,
		"stringToIntegerMD5": hashTransformer,
	}
	transformGeneratorMap = map[string]func(string) ColumnTranformer{
		"timestamp": genTimeFormat,
	}
	exportedTransformGenerators = []string{"f@timestamp@unix"}
)

// Probably want to change this to be a static type of error
func genError(offender interface{}, t string) error {
	return errors.New(fmt.Sprintf("Failed to parse %v as a %s", offender, t))
}

var (
	UnknownTransformError                 = errors.New("Unrecognized transform")
	ColumnNotFoundError                   = errors.New("Property Not Found")
	GeoIpDB               geoip.GeoLookup = loadDB()
)

func loadDB() geoip.GeoLookup {
	dbloc, asnloc := os.Getenv("GEO_IP_DB"), os.Getenv("ASN_IP_DB")
	g, load_err := geoip.NewGeoMMIp(dbloc, asnloc)
	if load_err != nil {
		log.Printf("Error loading geoip db at %s and %s: (%s), using noop db instead", dbloc, asnloc, load_err)
		return geoip.Noop()
	}
	return g
}

func SetGeoDB(geoloc string, asnloc string) error {
	g, load_err := geoip.NewGeoMMIp(geoloc, asnloc)
	if load_err != nil {
		return errors.New(fmt.Sprintf("Could not find geoIP db at %s or %s, using noop db instead\n",
			geoloc, asnloc))
	}
	GeoIpDB = g
	return nil
}

type ColumnTranformer func(interface{}) (string, error)

const (
	RedshiftDatetimeIngestString = "2006-01-02 15:04:05.999"
	fiveDigitYearCutoff          = 13140000000
	timeLowerBound               = 1000000000
	// Redshift and Go appear to differ on floating point representation
	// we use 10^-300 here as a stop gap estimation.
	FloatLowerBound = 10e-300
)

func intFormat(bitsAllowed uint) func(interface{}) (string, error) {
	maxIntAllowed := int64(1<<(bitsAllowed-1) - 1)
	minIntAllowed := int64(1<<(bitsAllowed-1)) * -1
	return func(target interface{}) (string, error) {
		// The json decoder we are using outputs as json.Number
		t, ok := target.(json.Number)
		var i int64
		var err error
		if !ok { // we should try parsing it from string
			strTarget, ok := target.(string)
			if !ok {
				err = errors.New("nil target")
			} else {
				i, err = strconv.ParseInt(strTarget, 10, 64)
			}
		} else {
			i, err = t.Int64()
		}
		if err != nil {
			return "", err
		}
		if i > maxIntAllowed || i < minIntAllowed {
			return "", fmt.Errorf("parsing \"%v\": value out of range (bits: %v)", i, bitsAllowed)
		}
		return strconv.FormatInt(i, 10), nil
	}
}

func floatFormat(target interface{}) (string, error) {
	t, ok := target.(json.Number)
	var f float64
	var err error
	if !ok { // we should try parsing it from string
		strTarget, ok := target.(string)
		if !ok {
			err = errors.New("nil target")
		} else {
			f, err = strconv.ParseFloat(strTarget, 64)
		}
	} else {
		f, err = t.Float64()
	}
	if err != nil {
		return "", err
	}
	if f < FloatLowerBound {
		f = 0.0
	}
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

func unixTimeFormat(target interface{}) (string, error) {
	t, ok := target.(json.Number)
	if !ok {
		return "", genError(target, "Time: unix")
	}
	i, err := t.Float64()
	if err != nil {
		return "", err
	}

	seconds := math.Trunc(i)
	nanos := (i - seconds) * float64(time.Second)
	// we also error if the year will be converted into a > 4 digit number
	if seconds < timeLowerBound || seconds > fiveDigitYearCutoff {
		return "", genError(target, "Time: unix")
	}
	return time.Unix(int64(seconds), int64(nanos)).In(PST).Format(RedshiftDatetimeIngestString), nil
}

func genTimeFormat(format string) ColumnTranformer {
	if format == "unix" {
		return unixTimeFormat
	}
	return func(target interface{}) (string, error) {
		str, ok := target.(string)
		if !ok {
			return "", genError(target, "Time: "+format)
		}
		t, err := time.ParseInLocation(format, str, PST)
		if err != nil {
			return "", err
		}
		return t.Format(RedshiftDatetimeIngestString), nil
	}
}

func varcharFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Varchar")
	}
	return str, nil
}

func boolFormat(target interface{}) (string, error) {
	b, ok := target.(bool)
	if ok {
		return fmt.Sprintf("%t", b), nil
	} // else we should try parsing it as a number
	i, ok := target.(json.Number)
	if ok {
		if i == json.Number("1") {
			return "true", nil
		} else if i == json.Number("0") {
			return "false", nil
		}
	}
	return "", genError(target, "Bool")
}

func ipCityFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Ip City")
	}
	return GeoIpDB.GetCity(str), nil
}

func ipCountryFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Ip Country")
	}
	return GeoIpDB.GetCountry(str), nil
}

func ipRegionFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Ip Region")
	}
	return GeoIpDB.GetRegion(str), nil
}

func ipAsnFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Ip Asn")
	}
	return GeoIpDB.GetAsn(str), nil
}

func ipAsnIntFormat(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Ip Asn")
	}
	asnString := GeoIpDB.GetAsn(str)
	index := strings.Index(asnString, " ")
	if index < 0 || !strings.HasPrefix(asnString, "AS") {
		return "", genError(target, "Ip Asn")
	}
	asnInt, err := strconv.Atoi(asnString[2:index])
	if err != nil {
		return "", genError(target, "Ip Asn")
	}
	return strconv.Itoa(asnInt), nil
}

func hashTransformer(target interface{}) (string, error) {
	str, ok := target.(string)
	if !ok {
		return "", genError(target, "Hash transformer")
	}
	bytedString := md5.Sum([]byte(str))
	hashedInt, err := strconv.ParseInt(hex.EncodeToString(bytedString[:])[:8], 16, 64)
	if err != nil {
		return "", genError(target, "Hash transformer")
	}
	return strconv.FormatInt(hashedInt, 10), nil
}
