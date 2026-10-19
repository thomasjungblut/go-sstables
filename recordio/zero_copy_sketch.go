package recordio

import (
	"fmt"
	"io"
)

// ReadNextInto is like ReadNext, but decodes the record into buf when it has enough capacity. The returned slice
// aliases buf in that case, otherwise a new slice is allocated. Passing the previous result back in makes
// sequential reads allocation free.
func (r *FileReader) ReadNextInto(buf []byte) ([]byte, error) {
	payload, err := r.readNextPayloadV4(func(n uint64) []byte { return grow(buf, n) })
	if err != nil || payload == nil || r.header.compressor == nil {
		return payload, err
	}
	// compressed payloads were read into the scratch buffer
	return r.header.compressor.DecompressWithBuf(payload, buf[:cap(buf)])
}

// ReadNextView is like ReadNext, but the returned slice is only valid until the next call on the reader and must not
// be modified. Fully buffered, uncompressed payloads are returned straight out of the read buffer.
func (r *FileReader) ReadNextView() ([]byte, error) {
	payload, err := r.readNextPayloadV4(nil)
	if err != nil || payload == nil || r.header.compressor == nil {
		return payload, err
	}
	r.viewScratch, err = r.header.compressor.DecompressWithBuf(payload, r.viewScratch)
	return r.viewScratch, err
}

// readNextPayloadV4 returns the raw (possibly compressed) payload. Uncompressed payloads are read into dst(n) when dst
// is given, compressed ones always go into the reader's scratch buffer. Without dst, fully buffered payloads are
// returned as a view on the read buffer.
func (r *FileReader) readNextPayloadV4(dst func(n uint64) []byte) ([]byte, error) {
	if !r.open || r.closed {
		return nil, fmt.Errorf("file reader for '%s' was either not opened yet or is closed already", r.file.Name())
	}

	start := r.reader.Count()
	payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := r.readRecordHeaderV4()
	if err != nil {
		// sketch: the DirectIO zero trailer handling of ReadNext is omitted
		return nil, err
	}
	if recordNil {
		r.currentOffset += r.reader.Count() - start
		return nil, nil
	}

	n := payloadSizeUncompressed
	if r.header.compressor != nil {
		n = payloadSizeCompressed
	}

	var payload []byte
	if dst == nil && n <= uint64(r.reader.Buffered()) {
		payload = r.reader.PeekBuffered(int(n))
		r.reader.DiscardBuffered(int(n))
	} else {
		if dst != nil && r.header.compressor == nil {
			payload = dst(n)
		} else {
			r.readScratch = grow(r.readScratch, n)
			payload = r.readScratch
		}
		if _, err := io.ReadFull(r.reader, payload); err != nil {
			return nil, fmt.Errorf("error while reading into record buffer of '%s': %w", r.file.Name(), err)
		}
	}

	r.currentOffset += r.reader.Count() - start
	return payload, nil
}

// ReadNextAtInto is like ReadNextAt, but decodes the record into buf when it has enough capacity. Thread-safe, as
// long as callers don't share buf.
func (r *MMapReader) ReadNextAtInto(offset uint64, buf []byte) ([]byte, error) {
	payload, payloadSizeUncompressed, err := r.payloadAtV4(offset)
	if err != nil || payload == nil {
		return nil, err
	}
	if r.header.compressor != nil {
		return r.header.compressor.DecompressWithBuf(payload, grow(buf, payloadSizeUncompressed))
	}
	buf = grow(buf, uint64(len(payload)))
	copy(buf, payload)
	return buf, nil
}

// ReadNextAtView is like ReadNextAt, but uncompressed records are returned as a view on the mapped file. The slice is
// only valid until Close and must not be modified, the mapping is read-only and writes will fault.
func (r *MMapReader) ReadNextAtView(offset uint64) ([]byte, error) {
	payload, payloadSizeUncompressed, err := r.payloadAtV4(offset)
	if err != nil || payload == nil {
		return nil, err
	}
	if r.header.compressor != nil {
		// no shared scratch buffer is possible on a concurrent reader, so compressed records are still allocated
		return r.header.compressor.DecompressWithBuf(payload, make([]byte, payloadSizeUncompressed))
	}
	// capping the capacity prevents appends from writing into the mapping
	return payload[:len(payload):len(payload)], nil
}

func (r *MMapReader) payloadAtV4(offset uint64) ([]byte, uint64, error) {
	data := r.mmapReaderSlice
	if offset >= uint64(len(data)) {
		return nil, 0, io.EOF
	}
	headerEnd := min(offset+RecordHeaderV4MaxSizeBytes, uint64(len(data)))
	payloadSizeUncompressed, payloadSizeCompressed, recordNil, headerLen, err := readRecordHeaderV4FromSlice(data[offset:headerEnd])
	if err != nil || recordNil {
		return nil, 0, err
	}
	n := payloadSizeUncompressed
	if r.header.compressor != nil {
		n = payloadSizeCompressed
	}
	payloadStart := offset + uint64(headerLen)
	if n > uint64(len(data))-payloadStart {
		return nil, 0, io.EOF
	}
	return data[payloadStart : payloadStart+n], payloadSizeUncompressed, nil
}

func grow(buf []byte, n uint64) []byte {
	if uint64(cap(buf)) >= n {
		return buf[:n]
	}
	return make([]byte, n)
}
