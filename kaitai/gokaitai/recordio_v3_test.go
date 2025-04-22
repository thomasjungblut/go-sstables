package gokaitai

import (
	"github.com/kaitai-io/kaitai_struct_go_runtime/kaitai"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/recordio"
	"os"
	"testing"
)

func TestHappyPathReadV3(t *testing.T) {
	path := "../../recordio/test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc"
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	rio := NewRecordioV3()
	err = rio.Read(kaitai.NewStream(f), nil, rio)
	require.NoError(t, err)

	require.Equal(t, uint32(3), rio.FileHeader.Version)
	require.Equal(t, RecordioV3_Compression__None, rio.FileHeader.CompressionType)
	for i := 0; i < len(rio.Record); i++ {
		record := rio.Record[i]
		require.Equal(t, i, len(record.Payload))
		require.Equal(t, recordio.MagicNumberSeparatorLongBytes, record.Magic)
		require.Equal(t, uint8(0), record.RecordNil)
	}
}
