package recordio

import (
	"encoding/binary"
	"fmt"
	"github.com/thomasjungblut/go-sstables/recordio/compressor"
)

type Header struct {
	compressionType int
	compressor      compressor.CompressionI
	fileVersion     uint32
}

func readFileHeaderFromBuffer(buffer []byte) (*Header, error) {
	if len(buffer) != FileHeaderSizeBytes {
		return nil, fmt.Errorf("file header buffer size mismatch, expected %d but was %d", FileHeaderSizeBytes, len(buffer))
	}

	fileVersion := binary.LittleEndian.Uint32(buffer[0:4])
	if fileVersion != Version {
		return nil, fmt.Errorf("version mismatch, expected %d but was %d", Version, fileVersion)
	}

	compressionType := binary.LittleEndian.Uint32(buffer[4:8])
	if compressionType > CompressionTypeSnappy {
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

func readRecordHeader(buffer []byte) (uint64, uint64, error) {
	if len(buffer) != RecordHeaderSizeBytes {
		return 0, 0, fmt.Errorf("record header buffer size mismatch, expected %d but was %d", RecordHeaderSizeBytes, len(buffer))
	}

	magicNumber := binary.LittleEndian.Uint32(buffer[0:4])
	if magicNumber != MagicNumberSeparator {
		return 0, 0, fmt.Errorf("magic number mismatch")
	}

	payloadSizeUncompressed := binary.LittleEndian.Uint64(buffer[4:12])
	payloadSizeCompressed := binary.LittleEndian.Uint64(buffer[12:20])
	return payloadSizeUncompressed, payloadSizeCompressed, nil
}

func allocateRecordBuffer(header *Header, payloadSizeUncompressed uint64, payloadSizeCompressed uint64) (uint64, []byte) {
	expectedBytesRead := payloadSizeUncompressed
	if header.compressor != nil {
		expectedBytesRead = payloadSizeCompressed
	}

	return expectedBytesRead, make([]byte, expectedBytesRead)
}
