package spade

import (
	"bytes"
	"encoding/base64"
)

// SpaceEncoding is to work around the fact that we decode `+` in query strings into ` `.
// https://www.w3.org/TR/html401/interact/forms.html#h-17.13.4.1 allows `+` to represent either
// since `+` is not a reserved character in RFC1738...
// It is the StdEncoding with ` ` substituting for `+`.
var SpaceEncoding = base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 /")

// DetermineBase64Encoding returns the encoding most likely to work on the given data.
func DetermineBase64Encoding(data []byte) *base64.Encoding {
	index := bytes.IndexAny(data, "-_ ")
	if index == -1 {
		return base64.StdEncoding
	}
	if data[index] == ' ' {
		return SpaceEncoding
	}
	return base64.URLEncoding
}
