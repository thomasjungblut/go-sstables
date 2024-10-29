package recordio

import (
	"bufio"
	"bytes"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"golang.org/x/exp/mmap"
	"io"
)

type MMapReader struct {
	mmapReader *mmap.ReaderAt
	header     *Header
	open       bool
	closed     bool
	bufferPool *pool.BufferPool
	path       string
}

func (r *MMapReader) Open() error {
	if r.open {
		return fmt.Errorf("mmap reader for '%s' is already opened", r.path)
	}

	if r.closed {
		return fmt.Errorf("mmap reader for '%s' is already closed", r.path)
	}

	buf := make([]byte, 8)
	numRead, err := r.mmapReader.ReadAt(buf, 0)
	if err != nil {
		return fmt.Errorf("failed reading at offset 0 in mmap reader for '%s': %w", r.path, err)
	}
	if numRead != len(buf) {
		return fmt.Errorf("not enough bytes in the header found, expected %d but were %d in mmap reader at '%s'", len(buf), numRead, r.path)
	}

	header, err := readFileHeaderFromBuffer(buf)
	if err != nil {
		return fmt.Errorf("failed reading header from buffer in mmap reader for '%s': %w", r.path, err)
	}

	r.header = header
	r.bufferPool = new(pool.BufferPool)
	r.open = true
	return nil
}

func (r *MMapReader) ReadNextAt(offset uint64) ([]byte, error) {
	if !r.open || r.closed {
		return nil, fmt.Errorf("reader at '%s' was either not opened yet or is closed already", r.path)
	}

	if r.header.fileVersion == Version1 {
		return readNextAtV1(r, offset)
	} else if r.header.fileVersion == Version2 {
		return readNextAtV2(r, offset)
	} else {
		headerBufPooled := r.bufferPool.Get(RecordHeaderV3MaxSizeBytes)
		defer r.bufferPool.Put(headerBufPooled)

		numRead, err := r.mmapReader.ReadAt(headerBufPooled, int64(offset))
		if err != nil {
			if err == io.EOF {
				// we'll only return EOF when we actually could not read anymore, that's different to the mmapReader semantics
				// which will return EOF when you have read less than the buffers actual size due to the EOF.
				// thankfully it's the same across the platforms they implement mmap for (unix mmap and windows umap file views).
				if numRead == 0 {
					return nil, io.EOF
				}
			} else {
				return nil, fmt.Errorf("failed reading at offset %d in mmap reader for '%s': %w", offset, r.path, err)
			}
		}

		headerByteReader := NewCountingByteReader(bufio.NewReader(bytes.NewReader(headerBufPooled[:numRead])))
		payloadSizeUncompressed, payloadSizeCompressed, recordNil, err := readRecordHeaderV3(headerByteReader)
		if err != nil {
			return nil, fmt.Errorf("failed reading record header at offset %d in mmap reader for '%s': %w", offset, r.path, err)
		}

		if recordNil {
			return nil, nil
		}

		expectedBytesRead, pooledRecordBuf := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
		defer r.bufferPool.Put(pooledRecordBuf)

		numRead, err = r.mmapReader.ReadAt(pooledRecordBuf, int64(offset)+int64(headerByteReader.Count()))
		if err != nil {
			return nil, fmt.Errorf("failed reading record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
		}

		if uint64(numRead) != expectedBytesRead {
			return nil, fmt.Errorf("not enough bytes in the record found in mmap reader '%s', expected %d but were %d", r.path, expectedBytesRead, numRead)
		}

		var returnSlice []byte
		if r.header.compressor != nil {
			pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
			defer r.bufferPool.Put(pooledDecompressionBuffer)

			decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuf, pooledDecompressionBuffer)
			if err != nil {
				return nil, fmt.Errorf("failed decompressing record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
			}
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(decompressedRecord))
			copy(returnSlice, decompressedRecord)
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuf))
			copy(returnSlice, pooledRecordBuf)
		}
		return returnSlice, nil
	}
}

func readNextAtV1(r *MMapReader, offset uint64) ([]byte, error) {
	headerBufPooled := r.bufferPool.Get(RecordHeaderSizeBytes)
	defer r.bufferPool.Put(headerBufPooled)

	numRead, err := r.mmapReader.ReadAt(headerBufPooled, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed reading at offset %d in mmap reader for '%s': %w", offset, r.path, err)
	}

	if numRead != len(headerBufPooled) {
		return nil, fmt.Errorf("not enough bytes in the record found in mmap reader '%s', expected %d but were %d", r.path, headerBufPooled, numRead)
	}

	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBufPooled)
	if err != nil {
		return nil, fmt.Errorf("failed reading record header at offset %d in mmap reader for '%s': %w", offset, r.path, err)
	}

	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = r.mmapReader.ReadAt(recordBuffer, int64(offset+RecordHeaderSizeBytes))
	if err != nil {
		return nil, fmt.Errorf("failed reading record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record found in mmap reader '%s', expected %d but were %d", r.path, expectedBytesRead, numRead)
	}

	if r.header.compressor != nil {
		recordBuffer, err = r.header.compressor.Decompress(recordBuffer)
		if err != nil {
			return nil, fmt.Errorf("failed decompressing record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
		}
	}
	return recordBuffer, nil
}

func readNextAtV2(r *MMapReader, offset uint64) ([]byte, error) {
	headerBufPooled := r.bufferPool.Get(RecordHeaderV3MaxSizeBytes)
	defer r.bufferPool.Put(headerBufPooled)

	numRead, err := r.mmapReader.ReadAt(headerBufPooled, int64(offset))
	if err != nil {
		if err == io.EOF {
			// we'll only return EOF when we actually could not read anymore, that's different to the mmapReader semantics
			// which will return EOF when you have read less than the buffers actual size due to the EOF.
			// thankfully it's the same across the platforms they implement mmap for (unix mmap and windows umap file views).
			if numRead == 0 {
				return nil, io.EOF
			}
		} else {
			return nil, fmt.Errorf("failed reading at offset %d in mmap reader for '%s': %w", offset, r.path, err)
		}
	}

	headerByteReader := NewCountingByteReader(bufio.NewReader(bytes.NewReader(headerBufPooled[:numRead])))
	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(headerByteReader)
	if err != nil {
		return nil, fmt.Errorf("failed reading record header at offset %d in mmap reader for '%s': %w", offset, r.path, err)
	}

	expectedBytesRead, pooledRecordBuf := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
	defer r.bufferPool.Put(pooledRecordBuf)

	numRead, err = r.mmapReader.ReadAt(pooledRecordBuf, int64(offset)+int64(headerByteReader.Count()))
	if err != nil {
		return nil, fmt.Errorf("failed reading record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record found in mmap reader '%s', expected %d but were %d", r.path, expectedBytesRead, numRead)
	}

	var returnSlice []byte
	if r.header.compressor != nil {
		pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
		defer r.bufferPool.Put(pooledDecompressionBuffer)

		decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuf, pooledDecompressionBuffer)
		if err != nil {
			return nil, fmt.Errorf("failed decompressing record at offset %d in mmap reader for '%s': %w", offset, r.path, err)
		}
		// we do a defensive copy here not to leak the pooled slice
		returnSlice = make([]byte, len(decompressedRecord))
		copy(returnSlice, decompressedRecord)
	} else {
		// we do a defensive copy here not to leak the pooled slice
		returnSlice = make([]byte, len(pooledRecordBuf))
		copy(returnSlice, pooledRecordBuf)
	}
	return returnSlice, nil
}

func (r *MMapReader) Close() error {
	r.closed = true
	r.open = false
	return r.mmapReader.Close()
}

// NewMemoryMappedReaderWithPath creates a new mmap reader at the given path.
func NewMemoryMappedReaderWithPath(path string) (ReadAtI, error) {
	mmapReaderAt, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error while opening mmap at '%s': %w", path, err)
	}
	return &MMapReader{mmapReader: mmapReaderAt, path: path}, nil
}
