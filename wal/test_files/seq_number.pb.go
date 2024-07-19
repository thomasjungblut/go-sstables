// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.27.2
// source: wal/test_files/seq_number.proto

package test_files

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SequenceNumber struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SequenceNumber uint64 `protobuf:"varint,1,opt,name=sequenceNumber,proto3" json:"sequenceNumber,omitempty"`
}

func (x *SequenceNumber) Reset() {
	*x = SequenceNumber{}
	if protoimpl.UnsafeEnabled {
		mi := &file_wal_test_files_seq_number_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SequenceNumber) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SequenceNumber) ProtoMessage() {}

func (x *SequenceNumber) ProtoReflect() protoreflect.Message {
	mi := &file_wal_test_files_seq_number_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SequenceNumber.ProtoReflect.Descriptor instead.
func (*SequenceNumber) Descriptor() ([]byte, []int) {
	return file_wal_test_files_seq_number_proto_rawDescGZIP(), []int{0}
}

func (x *SequenceNumber) GetSequenceNumber() uint64 {
	if x != nil {
		return x.SequenceNumber
	}
	return 0
}

var File_wal_test_files_seq_number_proto protoreflect.FileDescriptor

var file_wal_test_files_seq_number_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x77, 0x61, 0x6c, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73,
	0x2f, 0x73, 0x65, 0x71, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x0a, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x22, 0x38, 0x0a,
	0x0e, 0x53, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x12,
	0x26, 0x0a, 0x0e, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65,
	0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63,
	0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x68, 0x6f, 0x6d, 0x61, 0x73, 0x6a, 0x75, 0x6e, 0x67,
	0x62, 0x6c, 0x75, 0x74, 0x2f, 0x67, 0x6f, 0x2d, 0x73, 0x73, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73,
	0x2f, 0x77, 0x61, 0x6c, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_wal_test_files_seq_number_proto_rawDescOnce sync.Once
	file_wal_test_files_seq_number_proto_rawDescData = file_wal_test_files_seq_number_proto_rawDesc
)

func file_wal_test_files_seq_number_proto_rawDescGZIP() []byte {
	file_wal_test_files_seq_number_proto_rawDescOnce.Do(func() {
		file_wal_test_files_seq_number_proto_rawDescData = protoimpl.X.CompressGZIP(file_wal_test_files_seq_number_proto_rawDescData)
	})
	return file_wal_test_files_seq_number_proto_rawDescData
}

var file_wal_test_files_seq_number_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_wal_test_files_seq_number_proto_goTypes = []any{
	(*SequenceNumber)(nil), // 0: test_files.SequenceNumber
}
var file_wal_test_files_seq_number_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_wal_test_files_seq_number_proto_init() }
func file_wal_test_files_seq_number_proto_init() {
	if File_wal_test_files_seq_number_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_wal_test_files_seq_number_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*SequenceNumber); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_wal_test_files_seq_number_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_wal_test_files_seq_number_proto_goTypes,
		DependencyIndexes: file_wal_test_files_seq_number_proto_depIdxs,
		MessageInfos:      file_wal_test_files_seq_number_proto_msgTypes,
	}.Build()
	File_wal_test_files_seq_number_proto = out.File
	file_wal_test_files_seq_number_proto_rawDesc = nil
	file_wal_test_files_seq_number_proto_goTypes = nil
	file_wal_test_files_seq_number_proto_depIdxs = nil
}
