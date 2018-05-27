package bloomfilter

import _ "encoding/gob"

// conforms to interface gob.GobDecoder
func (f *Filter) GobDecode(data []byte) error {
	return f.UnmarshalBinary(data)
}

// conforms to interface gob.GobEncoder
func (f *Filter) GobEncode() ([]byte, error) {
	return f.MarshalBinary()
}
