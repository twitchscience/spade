package transformer

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/reporter"

	"github.com/twitchscience/spade/cache"
	"github.com/twitchscience/spade/lookup"
)

// Contains transformers to cast and munge properties coming in into types
// Consistent whith the incoming schemas.
//
// There are two types of transformers: Vanilla transformers, and transform generators.
// Transform generators are column transformer that require input from the
// config to determine how they parse things. The quintessential use case for this is
// for time transformers. Transform generators allow the user to define how
// the transformer should parse a inbound property.

// PST is the timezone used for everything.
var PST = getPST()

func getPST() *time.Location {
	pst, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	return pst
}

// MappingTransformerConfig contains the required configuration for a mapping transformer to work.
type MappingTransformerConfig struct {
	Fetcher     lookup.ValueFetcher // used to fetch a value with with support of extra columns
	LocalCache  cache.StringCache   // an in-memory cache to avoid fetching.
	RemoteCache cache.StringCache   // an external cache to avoid fetching.
	Stats       reporter.StatsLogger
}

// RedshiftType combines a way to get the input to the ColumnTransformer.
// Basically it performs Transformer(Event[EventProperty]) -> Column with the help of the values
// of the SupportingColumns provided.
type RedshiftType struct {
	Transformer       ColumnTransformer
	InboundName       string
	OutboundName      string
	SupportingColumns []string
}

// Format finds the column to transform and returns the outbound column name and transformed value.
func (r *RedshiftType) Format(eventProperties map[string]interface{}) (string, string, error) {
	args := make([]interface{}, 0, len(r.SupportingColumns)+1)
	columns := []string{r.InboundName}
	columns = append(columns, r.SupportingColumns...)
	for _, col := range columns {
		p, ok := eventProperties[col]
		if !ok && len(r.SupportingColumns) == 0 {
			return "", "", ErrColumnNotFound
		}
		args = append(args, p)
	}
	value, err := r.Transformer(args)
	return r.OutboundName, value, err
}

// GetSingleValueTransform returns us a single value Transformer for a given identifier string.
func GetSingleValueTransform(tType string) ColumnTransformer {
	if t, ok := singleValueTransformMap[tType]; ok {
		return safeColumnTransformer(t, 1)
	}
	if tType[0] == 'f' { // were building a transform function
		transformParams := strings.Split(tType, "@")
		if len(transformParams) < 3 {
			return nil
		}
		if transformGenerator, ok := singleValueTransformGeneratorMap[transformParams[1]]; ok {
			return safeColumnTransformer(transformGenerator(transformParams[2]), 1)
		}
		return nil
	}
	return nil
}

// GetMappingTransform returns a mapping transformer for a given identifier string.
func GetMappingTransform(tType string, config MappingTransformerConfig) ColumnTransformer {
	if transformGenerator, ok := mappingTransformMap[tType]; ok {
		return transformGenerator(config)
	}
	return nil
}

// New types should register here
var (
	singleValueTransformMap = map[string]ColumnTransformer{
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
	singleValueTransformGeneratorMap = map[string]func(string) ColumnTransformer{
		"timestamp": genTimeFormat,
	}
	mappingTransformMap = map[string]func(MappingTransformerConfig) ColumnTransformer{
		"userIDWithMapping": genLoginToIDTransformer,
	}
)

// Probably want to change this to be a static type of error
func genError(offender interface{}, t string) error {
	return fmt.Errorf("Failed to parse %v as a %s", offender, t)
}

var (
	// ErrUnknownTransform is when the transform from blueprint is unknown.
	ErrUnknownTransform = errors.New("Unrecognized transform")
	// ErrColumnNotFound is when a property from blueprint is not on an event.
	ErrColumnNotFound = errors.New("Property Not Found")
	// GeoIPDB is a Geo IP database, automatically kept updated.
	GeoIPDB = loadDB()
)

func loadDB() geoip.GeoLookup {
	dbloc, asnloc := os.Getenv("GEO_IP_DB"), os.Getenv("ASN_IP_DB")
	g, loadErr := geoip.NewGeoMMIp(dbloc, asnloc)
	if loadErr != nil {
		logger.WithError(loadErr).WithFields(map[string]interface{}{
			"db_location":  dbloc,
			"asn_location": asnloc,
		}).Error("Failed to load GeoIP DB, using no-op DB instead")
		return geoip.Noop()
	}
	return g
}

// SetGeoDB initializes the global GeoIPDB with the given DB locations.
func SetGeoDB(geoloc string, asnloc string) error {
	g, loadErr := geoip.NewGeoMMIp(geoloc, asnloc)
	if loadErr != nil {
		return fmt.Errorf("could not find geoIP db at %s or %s, using noop db instead",
			geoloc, asnloc)
	}
	GeoIPDB = g
	return nil
}

// ColumnTransformer takes an event property and transforms it to a string.
type ColumnTransformer func([]interface{}) (string, error)

const (
	// RedshiftDatetimeIngestString is the format of timestamps that Redshift understands.
	RedshiftDatetimeIngestString = "2006-01-02 15:04:05.999"
	fiveDigitYearCutoff          = 13140000000
	timeLowerBound               = 1000000000
	// FloatLowerBound is the minimum float value to allow.
	// Redshift and Go appear to differ on floating point representation
	// we use 10^-300 here as a stop gap estimation.
	FloatLowerBound = 10e-300
)

// SafeColumnTransformer generates a ColumnTransformer that calls the provided transformer only
// after validating that the amount of arguments provided at runtime is equal to nargs.
func safeColumnTransformer(transformer ColumnTransformer, nargs int) ColumnTransformer {
	return func(args []interface{}) (string, error) {
		if len(args) != nargs {
			return "", fmt.Errorf("Provide %v arguments instead of the required amount of %v",
				len(args), nargs)
		}
		return transformer(args)
	}
}

// safeParseInt safely extracts an int64 from an interface{}. It assumes first that it comes from a
// decoded json with UseNumber() enabled, otherwise it assumes is a string or just returns error.
func safeParseInt(value interface{}) (int64, error) {
	if t, ok := value.(json.Number); ok {
		return t.Int64()
	}
	if strTarget, ok := value.(string); ok {
		return strconv.ParseInt(strTarget, 10, 64)
	}
	return 0, errors.New("nil target")
}

func intFormat(bitsAllowed uint) func([]interface{}) (string, error) {
	maxIntAllowed := int64(1<<(bitsAllowed-1) - 1)
	minIntAllowed := int64(1<<(bitsAllowed-1)) * -1
	return func(args []interface{}) (string, error) {
		i, err := safeParseInt(args[0])
		if err != nil {
			return "", err
		}
		if i > maxIntAllowed || i < minIntAllowed {
			return "", fmt.Errorf("parsing \"%v\": value out of range (bits: %v)", i, bitsAllowed)
		}
		return strconv.FormatInt(i, 10), nil
	}
}

func floatFormat(args []interface{}) (string, error) {
	t, ok := args[0].(json.Number)
	var f float64
	var err error
	if !ok { // we should try parsing it from string
		strTarget, ok := args[0].(string)
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
	if math.IsNaN(f) {
		return "", nil
	}
	if -FloatLowerBound < f && f < FloatLowerBound {
		f = 0.0
	}
	return strconv.FormatFloat(f, 'f', -1, 64), nil
}

func genUnixTimeFormat(timezone *time.Location) ColumnTransformer {
	return func(args []interface{}) (string, error) {
		t, ok := args[0].(json.Number)
		if !ok {
			return "", genError(args[0], "Time: unix")
		}
		i, err := t.Float64()
		if err != nil {
			return "", err
		}

		seconds := math.Trunc(i)
		nanos := (i - seconds) * float64(time.Second)
		// we also error if the year will be converted into a > 4 digit number
		if seconds < timeLowerBound || seconds > fiveDigitYearCutoff {
			return "", genError(args[0], "Time: unix")
		}
		return time.Unix(int64(seconds), int64(nanos)).In(timezone).Format(RedshiftDatetimeIngestString), nil
	}
}

func genTimeFormat(format string) ColumnTransformer {
	if format == "unix" {
		return genUnixTimeFormat(PST)
	} else if format == "unix-utc" {
		return genUnixTimeFormat(time.UTC)
	}
	return func(args []interface{}) (string, error) {
		str, ok := args[0].(string)
		if !ok {
			return "", genError(args[0], "Time: "+format)
		}
		t, err := time.ParseInLocation(format, str, PST)
		if err != nil {
			return "", err
		}
		return t.Format(RedshiftDatetimeIngestString), nil
	}
}

func varcharFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Varchar")
	}
	return str, nil
}

func boolFormat(args []interface{}) (string, error) {
	b, ok := args[0].(bool)
	if ok {
		return fmt.Sprintf("%t", b), nil
	} // else we should try parsing it as a number
	i, ok := args[0].(json.Number)
	if ok {
		if i == json.Number("1") {
			return "true", nil
		} else if i == json.Number("0") {
			return "false", nil
		}
	}
	return "", genError(args[0], "Bool")
}

func ipCityFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Ip City")
	}
	return GeoIPDB.GetCity(str), nil
}

func ipCountryFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Ip Country")
	}
	return GeoIPDB.GetCountry(str), nil
}

func ipRegionFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Ip Region")
	}
	return GeoIPDB.GetRegion(str), nil
}

func ipAsnFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Ip Asn")
	}
	return GeoIPDB.GetAsn(str), nil
}

func ipAsnIntFormat(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Ip Asn")
	}
	asnString := GeoIPDB.GetAsn(str)
	if !strings.HasPrefix(asnString, "AS") {
		return "", genError(args[0], "Ip Asn")
	}
	index := strings.Index(asnString, " ")
	if index < 0 {
		index = len(asnString)
	}
	asnInt, err := strconv.Atoi(asnString[2:index])
	if err != nil {
		return "", genError(args[0], "Ip Asn")
	}
	return strconv.Itoa(asnInt), nil
}

func hashTransformer(args []interface{}) (string, error) {
	str, ok := args[0].(string)
	if !ok {
		return "", genError(args[0], "Hash transformer")
	}
	bytedString := md5.Sum([]byte(str))
	hashedInt, err := strconv.ParseInt(hex.EncodeToString(bytedString[:])[:8], 16, 64)
	if err != nil {
		return "", genError(args[0], "Hash transformer")
	}
	return strconv.FormatInt(hashedInt, 10), nil
}

func recordCacheError(stats reporter.StatsLogger, err error, operation string) {
	switch err {
	case nil:
		stats.IncrBy(fmt.Sprintf("transformer.login_to_id.cache_error.%s.success", operation), 1)
	case memcache.ErrCacheMiss:
		stats.IncrBy(fmt.Sprintf("transformer.login_to_id.cache_error.%s.cache_miss", operation), 1)
	case memcache.ErrMalformedKey:
		stats.IncrBy(fmt.Sprintf("transformer.login_to_id.cache_error.%s.malformed_key", operation), 1)
	default:
		if _, ok := err.(*memcache.ConnectTimeoutError); ok {
			stats.IncrBy(fmt.Sprintf("transformer.login_to_id.cache_error.%s.connect_timeout", operation), 1)
		} else {
			stats.IncrBy(fmt.Sprintf("transformer.login_to_id.cache_error.%s.other", operation), 1)
		}
	}
}

var (
	// ErrIDSet means we didn't have to do a lookup.
	ErrIDSet = errors.New("id was set")

	// ErrBadLookupValue means the lookup value is not usable.
	ErrBadLookupValue = errors.New("bad lookup value")

	// ErrEmptyLookupValue means the lookup value is not usable.
	ErrEmptyLookupValue = errors.New("empty lookup value")

	// ErrLocalCacheHit means the value was in the local cache.
	ErrLocalCacheHit = errors.New("local cache hit")

	// ErrRemoteCacheHit means the value was in the remote cache.
	ErrRemoteCacheHit = errors.New("remote cache hit")

	// ErrFetchSuccess means we were able to fetch the correct value.
	ErrFetchSuccess = errors.New("fetch success")

	// ErrFetchFailure means we were unable to fetch the correct value.
	ErrFetchFailure = errors.New("fetch failure")

	// ErrCacheSetFailure means we were unable to store the lookup value in the cache.
	ErrCacheSetFailure = errors.New("cache set failure")
)

func genLoginToIDTransformer(config MappingTransformerConfig) ColumnTransformer {
	return safeColumnTransformer(func(args []interface{}) (string, error) {
		// Relevant design decision:
		// We try to parse as a valid int64, if we fail we'll proceed to fetch. The important
		// point is that we will fetch in the event of any type of failure, so is not just an
		// empty string or null that will force the fetch. So a side effect of the transformer
		// is that it will pro actively try to fix invalid IDs
		localID, err := safeParseInt(args[0])
		if err == nil {
			return strconv.FormatInt(localID, 10), ErrIDSet
		}

		// We're assuming the second argument is a string representing a user login string which
		// we'll use to fetch the ID value
		login, ok := args[1].(string)
		if !ok {
			return "", ErrBadLookupValue
		}

		// No need to fetch if we have an empty login, let's just return an empty id
		if len(login) == 0 {
			return "", ErrEmptyLookupValue
		}

		// Chceck the local cache.
		cachedID, err := config.LocalCache.Get(login)
		if err == nil {
			recordCacheError(config.Stats, nil, "local_get")
			return cachedID, ErrLocalCacheHit
		}

		// Failed the local cache. Try the remote cache.
		cachedID, err = config.RemoteCache.Get(login)
		if err == nil {
			recordCacheError(config.Stats, nil, "remote_get")
			_ = config.LocalCache.Set(login, cachedID)
			return cachedID, ErrRemoteCacheHit
		}

		// We'll fetch at this point, remembering to save to cache before returning. One thing
		// to notice is that we'll always return failures from setting the cache in conjunction
		// with the fetched value, this way the client can identify failure to save to cache but
		// still use the value and move forward.
		fetchArgs := map[string]string{
			"login": login,
		}
		fetchedValue, err := config.Fetcher.FetchInt64(fetchArgs)
		if err != nil {
			if err == lookup.ErrExtractingValue {
				// This kind of error is most likely caused by an invalid login provided for
				// fetching, so let's cache an empty value so we don't keep fetching in the future
				_ = config.LocalCache.Set(login, "")
				err = config.RemoteCache.Set(login, "")
				recordCacheError(config.Stats, err, "remote_set")
			}
			return "", ErrFetchFailure
		}
		fetchedID := strconv.FormatInt(fetchedValue, 10)
		_ = config.LocalCache.Set(login, fetchedID)
		err = config.RemoteCache.Set(login, fetchedID)
		recordCacheError(config.Stats, err, "remote_set")
		if err != nil {
			return fetchedID, ErrCacheSetFailure
		}
		return fetchedID, ErrFetchSuccess
	}, 2)
}
