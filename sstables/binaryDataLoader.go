package sstables

import (
	"encoding/binary"
	"os"

	"github.com/thomasjungblut/go-sstables/recordio"
)

type BinaryDataLoader struct {
	dataFilePath string
	binaryFile   *os.File
}

func NewBinaryDataLoader() DataLoader {
	return &BinaryDataLoader{dataFilePath: ""}
}

func (bdl *BinaryDataLoader) Load(dataPath string) (recordio.ReadAtI, error) {
	bdl.dataFilePath = dataPath

	return bdl, nil
}

func (bdl *BinaryDataLoader) Close() error {
	return bdl.binaryFile.Close()
}

func (bdl *BinaryDataLoader) Open() error {
	binaryFile, err := os.Open(bdl.dataFilePath)
	if err != nil {
		return err
	}
	bdl.binaryFile = binaryFile
	return nil
}

func (bdl *BinaryDataLoader) ReadNextAt(offset uint64) ([]byte, error) {
	_, err := bdl.binaryFile.Seek(int64(offset), 0)
	var size uint64
	err = binary.Read(bdl.binaryFile, binary.LittleEndian, &size)
	if err != nil {
		return nil, err
	}
	val := make([]byte, size)
	err = binary.Read(bdl.binaryFile, binary.LittleEndian, &val)
	if err != nil {
		return nil, err
	}
	return val, nil
}
