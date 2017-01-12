package cache

// StringCache is a cache that operates over string keys and values.
type StringCache interface {
	Get(key string) (string, error)
	Set(key string, value string) error
}
