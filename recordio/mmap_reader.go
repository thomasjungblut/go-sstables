package recordio

import (
	"bufio"
	"bytes"
	"errors"
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
}

func (r *MMapReader) Open() error {
	if r.open {
		return errors.New("already opened")
	}

	if r.closed {
		return errors.New("already closed")
	}

	buf := make([]byte, 8)
	numRead, err := r.mmapReader.ReadAt(buf, 0)
	if err != nil {
		return err
	}
	if numRead != len(buf) {
		return fmt.Errorf("not enough bytes in the header found, expected %d but were %d", len(buf), numRead)
	}

	header, err := readFileHeaderFromBuffer(buf)
	if err != nil {
		return err
	}

	r.header = header
	r.bufferPool = new(pool.BufferPool)
	r.open = true
	return nil
}

func (r *MMapReader) ReadNextAt(offset uint64) ([]byte, error) {
	if !r.open || r.closed {
		return nil, errors.New("reader was either not opened yet or is closed already")
	}

	if r.header.fileVersion == Version1 {
		return readNextAtV1(r, offset)
	} else {
		headerBufPooled := r.bufferPool.Get(RecordHeaderV2MaxSizeBytes)

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
				return nil, err
			}
		}

		headerByteReader := NewCountingByteReader(bufio.NewReader(bytes.NewReader(headerBufPooled[:numRead])))
		payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV2(headerByteReader)
		if err != nil {
			return nil, err
		}

		r.bufferPool.Put(headerBufPooled)
		expectedBytesRead, pooledRecordBuf := allocateRecordBufferPooled(r.bufferPool, r.header, payloadSizeUncompressed, payloadSizeCompressed)
		numRead, err = r.mmapReader.ReadAt(pooledRecordBuf, int64(offset)+int64(headerByteReader.count))
		if err != nil {
			return nil, err
		}

		if uint64(numRead) != expectedBytesRead {
			return nil, fmt.Errorf("not enough bytes in the record found, expected %d but were %d", expectedBytesRead, numRead)
		}

		var returnSlice []byte
		if r.header.compressor != nil {
			pooledDecompressionBuffer := r.bufferPool.Get(int(payloadSizeUncompressed))
			decompressedRecord, err := r.header.compressor.DecompressWithBuf(pooledRecordBuf, pooledDecompressionBuffer)
			if err != nil {
				return nil, err
			}
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(decompressedRecord))
			copy(returnSlice, decompressedRecord)
			r.bufferPool.Put(pooledRecordBuf)
			r.bufferPool.Put(pooledDecompressionBuffer)
		} else {
			// we do a defensive copy here not to leak the pooled slice
			returnSlice = make([]byte, len(pooledRecordBuf))
			copy(returnSlice, pooledRecordBuf)
			r.bufferPool.Put(pooledRecordBuf)
		}
		return returnSlice, nil
	}
}

func readNextAtV1(r *MMapReader, offset uint64) ([]byte, error) {
	headerBufPooled := r.bufferPool.Get(RecordHeaderSizeBytes)
	numRead, err := r.mmapReader.ReadAt(headerBufPooled, int64(offset))
	if err != nil {
		return nil, err
	}

	if numRead != len(headerBufPooled) {
		return nil, fmt.Errorf("not enough bytes in the header found, expected %d but were %d",
			len(headerBufPooled), numRead)
	}

	payloadSizeUncompressed, payloadSizeCompressed, err := readRecordHeaderV1(headerBufPooled)
	if err != nil {
		return nil, err
	}

	r.bufferPool.Put(headerBufPooled)
	expectedBytesRead, recordBuffer := allocateRecordBuffer(r.header, payloadSizeUncompressed, payloadSizeCompressed)
	numRead, err = r.mmapReader.ReadAt(recordBuffer, int64(offset+RecordHeaderSizeBytes))
	if err != nil {
		return nil, err
	}

	if uint64(numRead) != expectedBytesRead {
		return nil, fmt.Errorf("not enough bytes in the record found, expected %d but were %d", expectedBytesRead, numRead)
	}

	if r.header.compressor != nil {
		recordBuffer, err = r.header.compressor.Decompress(recordBuffer)
		if err != nil {
			return nil, err
		}
	}
	return recordBuffer, nil
}

func (r *MMapReader) Close() error {
	r.closed = true
	r.open = false
	return r.mmapReader.Close()
}

func NewMemoryMappedReaderWithPath(path string) (*MMapReader, error) {
	mmapReaderAt, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	return &MMapReader{mmapReader: mmapReaderAt}, nil
}
