// Code generated by protoc-gen-go. DO NOT EDIT.
// source: sstables/proto/sstable.proto

package proto

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type IndexEntry struct {
	Key                  []byte   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ValueOffset          uint64   `protobuf:"varint,2,opt,name=valueOffset" json:"valueOffset,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *IndexEntry) Reset()         { *m = IndexEntry{} }
func (m *IndexEntry) String() string { return proto.CompactTextString(m) }
func (*IndexEntry) ProtoMessage()    {}
func (*IndexEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_sstable_ddca68e4db70d71e, []int{0}
}
func (m *IndexEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_IndexEntry.Unmarshal(m, b)
}
func (m *IndexEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_IndexEntry.Marshal(b, m, deterministic)
}
func (dst *IndexEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IndexEntry.Merge(dst, src)
}
func (m *IndexEntry) XXX_Size() int {
	return xxx_messageInfo_IndexEntry.Size(m)
}
func (m *IndexEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_IndexEntry.DiscardUnknown(m)
}

var xxx_messageInfo_IndexEntry proto.InternalMessageInfo

func (m *IndexEntry) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *IndexEntry) GetValueOffset() uint64 {
	if m != nil {
		return m.ValueOffset
	}
	return 0
}

type DataEntry struct {
	Value                []byte   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DataEntry) Reset()         { *m = DataEntry{} }
func (m *DataEntry) String() string { return proto.CompactTextString(m) }
func (*DataEntry) ProtoMessage()    {}
func (*DataEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_sstable_ddca68e4db70d71e, []int{1}
}
func (m *DataEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DataEntry.Unmarshal(m, b)
}
func (m *DataEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DataEntry.Marshal(b, m, deterministic)
}
func (dst *DataEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DataEntry.Merge(dst, src)
}
func (m *DataEntry) XXX_Size() int {
	return xxx_messageInfo_DataEntry.Size(m)
}
func (m *DataEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_DataEntry.DiscardUnknown(m)
}

var xxx_messageInfo_DataEntry proto.InternalMessageInfo

func (m *DataEntry) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*IndexEntry)(nil), "proto.IndexEntry")
	proto.RegisterType((*DataEntry)(nil), "proto.DataEntry")
}

func init() {
	proto.RegisterFile("sstables/proto/sstable.proto", fileDescriptor_sstable_ddca68e4db70d71e)
}

var fileDescriptor_sstable_ddca68e4db70d71e = []byte{
	// 127 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x29, 0x2e, 0x2e, 0x49,
	0x4c, 0xca, 0x49, 0x2d, 0xd6, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0xd7, 0x87, 0x72, 0xf5, 0xc0, 0x3c,
	0x21, 0x56, 0x30, 0xa5, 0xe4, 0xc0, 0xc5, 0xe5, 0x99, 0x97, 0x92, 0x5a, 0xe1, 0x9a, 0x57, 0x52,
	0x54, 0x29, 0x24, 0xc0, 0xc5, 0x9c, 0x9d, 0x5a, 0x29, 0xc1, 0xa8, 0xc0, 0xa8, 0xc1, 0x13, 0x04,
	0x62, 0x0a, 0x29, 0x70, 0x71, 0x97, 0x25, 0xe6, 0x94, 0xa6, 0xfa, 0xa7, 0xa5, 0x15, 0xa7, 0x96,
	0x48, 0x30, 0x29, 0x30, 0x6a, 0xb0, 0x04, 0x21, 0x0b, 0x29, 0x29, 0x72, 0x71, 0xba, 0x24, 0x96,
	0x24, 0x42, 0x0c, 0x10, 0xe1, 0x62, 0x05, 0xcb, 0x41, 0x8d, 0x80, 0x70, 0x92, 0xd8, 0xc0, 0x76,
	0x19, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0xf0, 0xeb, 0xe6, 0x74, 0x92, 0x00, 0x00, 0x00,
}