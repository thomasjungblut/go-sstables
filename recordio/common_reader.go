package recordio

import (
	"encoding/binary"
	"fmt"

	"io"

	pool "capnproto.org/go/capnp/v3/exp/bufferpool"
	"github.com/thomasjungblut/go-sstables/recordio/compressor"
)

type Header struct {
	compressionType int
	compressor      compressor.CompressionI
	fileVersion     uint32
}

var MagicNumberMismatchErr = fmt.Errorf("magic number mismatch")

func readFileHeaderFromBuffer(buffer []byte) (*Header, error) {
	if len(buffer) != FileHeaderSizeBytes {
		return nil, fmt.Errorf("file header buffer size mismatch, expected %d but was %d", FileHeaderSizeBytes, len(buffer))
	}

	fileVersion := binary.LittleEndian.Uint32(buffer[0:4])
	if fileVersion > CurrentVersion || fileVersion < Version1 {
		return nil, fmt.Errorf("version mismatch, expected a value from %d to %d but was %d", Version1, CurrentVersion, fileVersion)
	}

	compressionType := binary.LittleEndian.Uint32(buffer[4:8])
	if compressionType > CompressionTypeLzw {
		return nil, fmt.Errorf("unknown compression type [%d]", compressionType)
	}

	header := &Header{compressionType: int(compressionType), fileVersion: fileVersion}
	cmp, err := NewCompressorForType(header.compressionType)
	if err != nil {
		return nil, err
	}
	header.compressor = cmp
	return header, nil
}

func readRecordHeaderV1(buffer []byte) (uint64, uint64, error) {
	if len(buffer) != RecordHeaderSizeBytesV1V2 {
		return 0, 0, fmt.Errorf("record header buffer size mismatch, expected %d but was %d", RecordHeaderSizeBytesV1V2, len(buffer))
	}

	magicNumber := binary.LittleEndian.Uint32(buffer[0:4])
	if magicNumber != MagicNumberSeparator {
		return 0, 0, MagicNumberMismatchErr
	}

	payloadSizeUncompressed := binary.LittleEndian.Uint64(buffer[4:12])
	payloadSizeCompressed := binary.LittleEndian.Uint64(buffer[12:20])
	return payloadSizeUncompressed, payloadSizeCompressed, nil
}

func readRecordHeaderV2(r io.ByteReader) (uint64, uint64, error) {
	magicNumber, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, err
	}
	if magicNumber != MagicNumberSeparatorLong {
		return 0, 0, MagicNumberMismatchErr
	}

	payloadSizeUncompressed, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, err
	}

	payloadSizeCompressed, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, err
	}

	return payloadSizeUncompressed, payloadSizeCompressed, nil
}

func readRecordHeaderV3(r io.ByteReader) (uint64, uint64, bool, error) {
	// TODO(thomas): V4 will need some kind of CRC hash to ensure that this header is valid
	// currently we can read a valid magic number inside some data chunk, and a totally invalid
	// remainder of the header e.g. a nil record bit or an uncompressed run-length that's huge and going past EOF.

	magicNumber, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, false, err
	}
	if magicNumber != MagicNumberSeparatorLong {
		return 0, 0, false, MagicNumberMismatchErr
	}

	recordNil, err := r.ReadByte()
	if err != nil {
		return 0, 0, false, err
	}

	payloadSizeUncompressed, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, false, err
	}

	payloadSizeCompressed, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, false, err
	}

	return payloadSizeUncompressed, payloadSizeCompressed, recordNil == 1, nil
}

func allocateRecordBuffer(header *Header, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (uint64, []byte) {
	expectedBytesRead := payloadSizeUncompressed
	if header.compressor != nil {
		expectedBytesRead = payloadSizeCompressed
	}

	return expectedBytesRead, make([]byte, expectedBytesRead)
}

func allocateRecordBufferPooled(bufferPool *pool.Pool, header *Header, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (uint64, []byte) {
	expectedBytesRead := payloadSizeUncompressed
	if header.compressor != nil {
		expectedBytesRead = payloadSizeCompressed
	}

	return expectedBytesRead, bufferPool.Get(int(expectedBytesRead))
}

func copyBuf(b []byte) []byte {
	bx := make([]byte, len(b))
	copy(bx, b)
	return bx
}
