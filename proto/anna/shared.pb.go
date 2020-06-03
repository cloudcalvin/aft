// Code generated by protoc-gen-go. DO NOT EDIT.
// source: shared.proto

package anna

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// An arbitrary set of strings; used for a variety of purposes across the
// system.
type StringSet struct {
	// An unordered set of keys.
	Keys                 []string `protobuf:"bytes,1,rep,name=keys,proto3" json:"keys,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *StringSet) Reset()         { *m = StringSet{} }
func (m *StringSet) String() string { return proto.CompactTextString(m) }
func (*StringSet) ProtoMessage()    {}
func (*StringSet) Descriptor() ([]byte, []int) {
	return fileDescriptor_d8a4e87e678c5ced, []int{0}
}

func (m *StringSet) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StringSet.Unmarshal(m, b)
}
func (m *StringSet) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StringSet.Marshal(b, m, deterministic)
}
func (m *StringSet) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StringSet.Merge(m, src)
}
func (m *StringSet) XXX_Size() int {
	return xxx_messageInfo_StringSet.Size(m)
}
func (m *StringSet) XXX_DiscardUnknown() {
	xxx_messageInfo_StringSet.DiscardUnknown(m)
}

var xxx_messageInfo_StringSet proto.InternalMessageInfo

func (m *StringSet) GetKeys() []string {
	if m != nil {
		return m.Keys
	}
	return nil
}

// A message representing a pointer to a particular version of a particular
// key.
type KeyVersion struct {
	// The name of the key we are referencing.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// A vector clock for the version of the key we are referencing.
	VectorClock          map[string]uint32 `protobuf:"bytes,2,rep,name=vector_clock,json=vectorClock,proto3" json:"vector_clock,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *KeyVersion) Reset()         { *m = KeyVersion{} }
func (m *KeyVersion) String() string { return proto.CompactTextString(m) }
func (*KeyVersion) ProtoMessage()    {}
func (*KeyVersion) Descriptor() ([]byte, []int) {
	return fileDescriptor_d8a4e87e678c5ced, []int{1}
}

func (m *KeyVersion) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyVersion.Unmarshal(m, b)
}
func (m *KeyVersion) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyVersion.Marshal(b, m, deterministic)
}
func (m *KeyVersion) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyVersion.Merge(m, src)
}
func (m *KeyVersion) XXX_Size() int {
	return xxx_messageInfo_KeyVersion.Size(m)
}
func (m *KeyVersion) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyVersion.DiscardUnknown(m)
}

var xxx_messageInfo_KeyVersion proto.InternalMessageInfo

func (m *KeyVersion) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyVersion) GetVectorClock() map[string]uint32 {
	if m != nil {
		return m.VectorClock
	}
	return nil
}

// A wrapper message for a list of KeyVersions.
type KeyVersionList struct {
	// The list of KeyVersion references.
	Keys                 []*KeyVersion `protobuf:"bytes,1,rep,name=keys,proto3" json:"keys,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *KeyVersionList) Reset()         { *m = KeyVersionList{} }
func (m *KeyVersionList) String() string { return proto.CompactTextString(m) }
func (*KeyVersionList) ProtoMessage()    {}
func (*KeyVersionList) Descriptor() ([]byte, []int) {
	return fileDescriptor_d8a4e87e678c5ced, []int{2}
}

func (m *KeyVersionList) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyVersionList.Unmarshal(m, b)
}
func (m *KeyVersionList) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyVersionList.Marshal(b, m, deterministic)
}
func (m *KeyVersionList) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyVersionList.Merge(m, src)
}
func (m *KeyVersionList) XXX_Size() int {
	return xxx_messageInfo_KeyVersionList.Size(m)
}
func (m *KeyVersionList) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyVersionList.DiscardUnknown(m)
}

var xxx_messageInfo_KeyVersionList proto.InternalMessageInfo

func (m *KeyVersionList) GetKeys() []*KeyVersion {
	if m != nil {
		return m.Keys
	}
	return nil
}

func init() {
	proto.RegisterType((*StringSet)(nil), "StringSet")
	proto.RegisterType((*KeyVersion)(nil), "KeyVersion")
	proto.RegisterMapType((map[string]uint32)(nil), "KeyVersion.VectorClockEntry")
	proto.RegisterType((*KeyVersionList)(nil), "KeyVersionList")
}

func init() { proto.RegisterFile("shared.proto", fileDescriptor_d8a4e87e678c5ced) }

var fileDescriptor_d8a4e87e678c5ced = []byte{
	// 199 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x29, 0xce, 0x48, 0x2c,
	0x4a, 0x4d, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x57, 0x92, 0xe7, 0xe2, 0x0c, 0x2e, 0x29, 0xca,
	0xcc, 0x4b, 0x0f, 0x4e, 0x2d, 0x11, 0x12, 0xe2, 0x62, 0xc9, 0x4e, 0xad, 0x2c, 0x96, 0x60, 0x54,
	0x60, 0xd6, 0xe0, 0x0c, 0x02, 0xb3, 0x95, 0xe6, 0x33, 0x72, 0x71, 0x79, 0xa7, 0x56, 0x86, 0xa5,
	0x16, 0x15, 0x67, 0xe6, 0xe7, 0x09, 0x09, 0x70, 0x31, 0x67, 0xa7, 0x56, 0x4a, 0x30, 0x2a, 0x30,
	0x6a, 0x70, 0x06, 0x81, 0x98, 0x42, 0xf6, 0x5c, 0x3c, 0x65, 0xa9, 0xc9, 0x25, 0xf9, 0x45, 0xf1,
	0xc9, 0x39, 0xf9, 0xc9, 0xd9, 0x12, 0x4c, 0x0a, 0xcc, 0x1a, 0xdc, 0x46, 0x32, 0x7a, 0x08, 0x4d,
	0x7a, 0x61, 0x60, 0x79, 0x67, 0x90, 0xb4, 0x6b, 0x5e, 0x49, 0x51, 0x65, 0x10, 0x77, 0x19, 0x42,
	0x44, 0xca, 0x8e, 0x4b, 0x00, 0x5d, 0x01, 0x16, 0x6b, 0x44, 0xb8, 0x58, 0xcb, 0x12, 0x73, 0x4a,
	0x53, 0x25, 0x98, 0x14, 0x18, 0x35, 0x78, 0x83, 0x20, 0x1c, 0x2b, 0x26, 0x0b, 0x46, 0x25, 0x43,
	0x2e, 0x3e, 0x84, 0x5d, 0x3e, 0x99, 0xc5, 0x25, 0x42, 0xf2, 0x48, 0xfe, 0xe0, 0x36, 0xe2, 0x46,
	0x72, 0x0a, 0xc4, 0x53, 0x49, 0x6c, 0x60, 0xcf, 0x1b, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x74,
	0x58, 0x9a, 0xc8, 0x0c, 0x01, 0x00, 0x00,
}
