// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.17.3
// source: mq.proto

package mq_pb

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

// ////////////////////////////////////////////////
type SegmentInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Segment          *Segment `protobuf:"bytes,1,opt,name=segment,proto3" json:"segment,omitempty"`
	StartTsNs        int64    `protobuf:"varint,2,opt,name=start_ts_ns,json=startTsNs,proto3" json:"start_ts_ns,omitempty"`
	Brokers          []string `protobuf:"bytes,3,rep,name=brokers,proto3" json:"brokers,omitempty"`
	StopTsNs         int64    `protobuf:"varint,4,opt,name=stop_ts_ns,json=stopTsNs,proto3" json:"stop_ts_ns,omitempty"`
	PreviousSegments []int32  `protobuf:"varint,5,rep,packed,name=previous_segments,json=previousSegments,proto3" json:"previous_segments,omitempty"`
	NextSegments     []int32  `protobuf:"varint,6,rep,packed,name=next_segments,json=nextSegments,proto3" json:"next_segments,omitempty"`
}

func (x *SegmentInfo) Reset() {
	*x = SegmentInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SegmentInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SegmentInfo) ProtoMessage() {}

func (x *SegmentInfo) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SegmentInfo.ProtoReflect.Descriptor instead.
func (*SegmentInfo) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{0}
}

func (x *SegmentInfo) GetSegment() *Segment {
	if x != nil {
		return x.Segment
	}
	return nil
}

func (x *SegmentInfo) GetStartTsNs() int64 {
	if x != nil {
		return x.StartTsNs
	}
	return 0
}

func (x *SegmentInfo) GetBrokers() []string {
	if x != nil {
		return x.Brokers
	}
	return nil
}

func (x *SegmentInfo) GetStopTsNs() int64 {
	if x != nil {
		return x.StopTsNs
	}
	return 0
}

func (x *SegmentInfo) GetPreviousSegments() []int32 {
	if x != nil {
		return x.PreviousSegments
	}
	return nil
}

func (x *SegmentInfo) GetNextSegments() []int32 {
	if x != nil {
		return x.NextSegments
	}
	return nil
}

type FindBrokerLeaderRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FilerGroup string `protobuf:"bytes,1,opt,name=filer_group,json=filerGroup,proto3" json:"filer_group,omitempty"`
}

func (x *FindBrokerLeaderRequest) Reset() {
	*x = FindBrokerLeaderRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FindBrokerLeaderRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FindBrokerLeaderRequest) ProtoMessage() {}

func (x *FindBrokerLeaderRequest) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FindBrokerLeaderRequest.ProtoReflect.Descriptor instead.
func (*FindBrokerLeaderRequest) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{1}
}

func (x *FindBrokerLeaderRequest) GetFilerGroup() string {
	if x != nil {
		return x.FilerGroup
	}
	return ""
}

type FindBrokerLeaderResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Broker string `protobuf:"bytes,1,opt,name=broker,proto3" json:"broker,omitempty"`
}

func (x *FindBrokerLeaderResponse) Reset() {
	*x = FindBrokerLeaderResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FindBrokerLeaderResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FindBrokerLeaderResponse) ProtoMessage() {}

func (x *FindBrokerLeaderResponse) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FindBrokerLeaderResponse.ProtoReflect.Descriptor instead.
func (*FindBrokerLeaderResponse) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{2}
}

func (x *FindBrokerLeaderResponse) GetBroker() string {
	if x != nil {
		return x.Broker
	}
	return ""
}

type Partition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RingSize   int32 `protobuf:"varint,1,opt,name=ring_size,json=ringSize,proto3" json:"ring_size,omitempty"`
	RangeStart int32 `protobuf:"varint,2,opt,name=range_start,json=rangeStart,proto3" json:"range_start,omitempty"`
	RangeStop  int32 `protobuf:"varint,3,opt,name=range_stop,json=rangeStop,proto3" json:"range_stop,omitempty"`
}

func (x *Partition) Reset() {
	*x = Partition{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Partition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Partition) ProtoMessage() {}

func (x *Partition) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Partition.ProtoReflect.Descriptor instead.
func (*Partition) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{3}
}

func (x *Partition) GetRingSize() int32 {
	if x != nil {
		return x.RingSize
	}
	return 0
}

func (x *Partition) GetRangeStart() int32 {
	if x != nil {
		return x.RangeStart
	}
	return 0
}

func (x *Partition) GetRangeStop() int32 {
	if x != nil {
		return x.RangeStop
	}
	return 0
}

type Segment struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Namespace string     `protobuf:"bytes,1,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Topic     string     `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
	Id        int32      `protobuf:"varint,3,opt,name=id,proto3" json:"id,omitempty"`
	Partition *Partition `protobuf:"bytes,4,opt,name=partition,proto3" json:"partition,omitempty"`
}

func (x *Segment) Reset() {
	*x = Segment{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Segment) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Segment) ProtoMessage() {}

func (x *Segment) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Segment.ProtoReflect.Descriptor instead.
func (*Segment) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{4}
}

func (x *Segment) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *Segment) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

func (x *Segment) GetId() int32 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Segment) GetPartition() *Partition {
	if x != nil {
		return x.Partition
	}
	return nil
}

type AssignSegmentBrokersRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Segment *Segment `protobuf:"bytes,1,opt,name=segment,proto3" json:"segment,omitempty"`
}

func (x *AssignSegmentBrokersRequest) Reset() {
	*x = AssignSegmentBrokersRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AssignSegmentBrokersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AssignSegmentBrokersRequest) ProtoMessage() {}

func (x *AssignSegmentBrokersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AssignSegmentBrokersRequest.ProtoReflect.Descriptor instead.
func (*AssignSegmentBrokersRequest) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{5}
}

func (x *AssignSegmentBrokersRequest) GetSegment() *Segment {
	if x != nil {
		return x.Segment
	}
	return nil
}

type AssignSegmentBrokersResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Brokers []string `protobuf:"bytes,1,rep,name=brokers,proto3" json:"brokers,omitempty"`
}

func (x *AssignSegmentBrokersResponse) Reset() {
	*x = AssignSegmentBrokersResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AssignSegmentBrokersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AssignSegmentBrokersResponse) ProtoMessage() {}

func (x *AssignSegmentBrokersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AssignSegmentBrokersResponse.ProtoReflect.Descriptor instead.
func (*AssignSegmentBrokersResponse) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{6}
}

func (x *AssignSegmentBrokersResponse) GetBrokers() []string {
	if x != nil {
		return x.Brokers
	}
	return nil
}

type CheckSegmentStatusRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Segment *Segment `protobuf:"bytes,1,opt,name=segment,proto3" json:"segment,omitempty"`
}

func (x *CheckSegmentStatusRequest) Reset() {
	*x = CheckSegmentStatusRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckSegmentStatusRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckSegmentStatusRequest) ProtoMessage() {}

func (x *CheckSegmentStatusRequest) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckSegmentStatusRequest.ProtoReflect.Descriptor instead.
func (*CheckSegmentStatusRequest) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{7}
}

func (x *CheckSegmentStatusRequest) GetSegment() *Segment {
	if x != nil {
		return x.Segment
	}
	return nil
}

type CheckSegmentStatusResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IsActive bool `protobuf:"varint,1,opt,name=is_active,json=isActive,proto3" json:"is_active,omitempty"`
}

func (x *CheckSegmentStatusResponse) Reset() {
	*x = CheckSegmentStatusResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckSegmentStatusResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckSegmentStatusResponse) ProtoMessage() {}

func (x *CheckSegmentStatusResponse) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckSegmentStatusResponse.ProtoReflect.Descriptor instead.
func (*CheckSegmentStatusResponse) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{8}
}

func (x *CheckSegmentStatusResponse) GetIsActive() bool {
	if x != nil {
		return x.IsActive
	}
	return false
}

type CheckBrokerLoadRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CheckBrokerLoadRequest) Reset() {
	*x = CheckBrokerLoadRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckBrokerLoadRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckBrokerLoadRequest) ProtoMessage() {}

func (x *CheckBrokerLoadRequest) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckBrokerLoadRequest.ProtoReflect.Descriptor instead.
func (*CheckBrokerLoadRequest) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{9}
}

type CheckBrokerLoadResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MessageCount int64 `protobuf:"varint,1,opt,name=message_count,json=messageCount,proto3" json:"message_count,omitempty"`
	BytesCount   int64 `protobuf:"varint,2,opt,name=bytes_count,json=bytesCount,proto3" json:"bytes_count,omitempty"`
}

func (x *CheckBrokerLoadResponse) Reset() {
	*x = CheckBrokerLoadResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckBrokerLoadResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckBrokerLoadResponse) ProtoMessage() {}

func (x *CheckBrokerLoadResponse) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckBrokerLoadResponse.ProtoReflect.Descriptor instead.
func (*CheckBrokerLoadResponse) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{10}
}

func (x *CheckBrokerLoadResponse) GetMessageCount() int64 {
	if x != nil {
		return x.MessageCount
	}
	return 0
}

func (x *CheckBrokerLoadResponse) GetBytesCount() int64 {
	if x != nil {
		return x.BytesCount
	}
	return 0
}

// ////////////////////////////////////////////////
type PublishRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Init    *PublishRequest_InitMessage `protobuf:"bytes,1,opt,name=init,proto3" json:"init,omitempty"`
	Message []byte                      `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PublishRequest) Reset() {
	*x = PublishRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishRequest) ProtoMessage() {}

func (x *PublishRequest) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishRequest.ProtoReflect.Descriptor instead.
func (*PublishRequest) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{11}
}

func (x *PublishRequest) GetInit() *PublishRequest_InitMessage {
	if x != nil {
		return x.Init
	}
	return nil
}

func (x *PublishRequest) GetMessage() []byte {
	if x != nil {
		return x.Message
	}
	return nil
}

type PublishResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AckSequence int64 `protobuf:"varint,1,opt,name=ack_sequence,json=ackSequence,proto3" json:"ack_sequence,omitempty"`
	IsClosed    bool  `protobuf:"varint,2,opt,name=is_closed,json=isClosed,proto3" json:"is_closed,omitempty"`
}

func (x *PublishResponse) Reset() {
	*x = PublishResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[12]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishResponse) ProtoMessage() {}

func (x *PublishResponse) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[12]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishResponse.ProtoReflect.Descriptor instead.
func (*PublishResponse) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{12}
}

func (x *PublishResponse) GetAckSequence() int64 {
	if x != nil {
		return x.AckSequence
	}
	return 0
}

func (x *PublishResponse) GetIsClosed() bool {
	if x != nil {
		return x.IsClosed
	}
	return false
}

type PublishRequest_InitMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Segment *Segment `protobuf:"bytes,1,opt,name=segment,proto3" json:"segment,omitempty"`
}

func (x *PublishRequest_InitMessage) Reset() {
	*x = PublishRequest_InitMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_mq_proto_msgTypes[13]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishRequest_InitMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishRequest_InitMessage) ProtoMessage() {}

func (x *PublishRequest_InitMessage) ProtoReflect() protoreflect.Message {
	mi := &file_mq_proto_msgTypes[13]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishRequest_InitMessage.ProtoReflect.Descriptor instead.
func (*PublishRequest_InitMessage) Descriptor() ([]byte, []int) {
	return file_mq_proto_rawDescGZIP(), []int{11, 0}
}

func (x *PublishRequest_InitMessage) GetSegment() *Segment {
	if x != nil {
		return x.Segment
	}
	return nil
}

var File_mq_proto protoreflect.FileDescriptor

var file_mq_proto_rawDesc = []byte{
	0x0a, 0x08, 0x6d, 0x71, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0c, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x22, 0xe8, 0x01, 0x0a, 0x0b, 0x53, 0x65, 0x67,
	0x6d, 0x65, 0x6e, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x2f, 0x0a, 0x07, 0x73, 0x65, 0x67, 0x6d,
	0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74,
	0x52, 0x07, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x1e, 0x0a, 0x0b, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x5f, 0x74, 0x73, 0x5f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09,
	0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73, 0x4e, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x62, 0x72, 0x6f,
	0x6b, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x62, 0x72, 0x6f, 0x6b,
	0x65, 0x72, 0x73, 0x12, 0x1c, 0x0a, 0x0a, 0x73, 0x74, 0x6f, 0x70, 0x5f, 0x74, 0x73, 0x5f, 0x6e,
	0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x73, 0x74, 0x6f, 0x70, 0x54, 0x73, 0x4e,
	0x73, 0x12, 0x2b, 0x0a, 0x11, 0x70, 0x72, 0x65, 0x76, 0x69, 0x6f, 0x75, 0x73, 0x5f, 0x73, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x05, 0x52, 0x10, 0x70, 0x72,
	0x65, 0x76, 0x69, 0x6f, 0x75, 0x73, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x23,
	0x0a, 0x0d, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x18,
	0x06, 0x20, 0x03, 0x28, 0x05, 0x52, 0x0c, 0x6e, 0x65, 0x78, 0x74, 0x53, 0x65, 0x67, 0x6d, 0x65,
	0x6e, 0x74, 0x73, 0x22, 0x3a, 0x0a, 0x17, 0x46, 0x69, 0x6e, 0x64, 0x42, 0x72, 0x6f, 0x6b, 0x65,
	0x72, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1f,
	0x0a, 0x0b, 0x66, 0x69, 0x6c, 0x65, 0x72, 0x5f, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0a, 0x66, 0x69, 0x6c, 0x65, 0x72, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x22,
	0x32, 0x0a, 0x18, 0x46, 0x69, 0x6e, 0x64, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x62,
	0x72, 0x6f, 0x6b, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x62, 0x72, 0x6f,
	0x6b, 0x65, 0x72, 0x22, 0x68, 0x0a, 0x09, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x12, 0x1b, 0x0a, 0x09, 0x72, 0x69, 0x6e, 0x67, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x08, 0x72, 0x69, 0x6e, 0x67, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x1f, 0x0a,
	0x0b, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x0a, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x53, 0x74, 0x61, 0x72, 0x74, 0x12, 0x1d,
	0x0a, 0x0a, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x73, 0x74, 0x6f, 0x70, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x09, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x53, 0x74, 0x6f, 0x70, 0x22, 0x84, 0x01,
	0x0a, 0x07, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x02, 0x69, 0x64, 0x12, 0x35, 0x0a,
	0x09, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x17, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x09, 0x70, 0x61, 0x72, 0x74, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x22, 0x4e, 0x0a, 0x1b, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x53, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x2f, 0x0a, 0x07, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67,
	0x5f, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x07, 0x73, 0x65, 0x67,
	0x6d, 0x65, 0x6e, 0x74, 0x22, 0x38, 0x0a, 0x1c, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x53, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x73, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x07, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x73, 0x22, 0x4c,
	0x0a, 0x19, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2f, 0x0a, 0x07, 0x73,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x67, 0x6d,
	0x65, 0x6e, 0x74, 0x52, 0x07, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x39, 0x0a, 0x1a,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x69, 0x73,
	0x5f, 0x61, 0x63, 0x74, 0x69, 0x76, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x69,
	0x73, 0x41, 0x63, 0x74, 0x69, 0x76, 0x65, 0x22, 0x18, 0x0a, 0x16, 0x43, 0x68, 0x65, 0x63, 0x6b,
	0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x6f, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x22, 0x5f, 0x0a, 0x17, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72,
	0x4c, 0x6f, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x23, 0x0a, 0x0d,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0c, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x43, 0x6f, 0x75, 0x6e,
	0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x62, 0x79, 0x74, 0x65, 0x73, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x62, 0x79, 0x74, 0x65, 0x73, 0x43, 0x6f, 0x75,
	0x6e, 0x74, 0x22, 0xa8, 0x01, 0x0a, 0x0e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3c, 0x0a, 0x04, 0x69, 0x6e, 0x69, 0x74, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f,
	0x70, 0x62, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x2e, 0x49, 0x6e, 0x69, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x52, 0x04, 0x69,
	0x6e, 0x69, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x1a, 0x3e, 0x0a,
	0x0b, 0x49, 0x6e, 0x69, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x2f, 0x0a, 0x07,
	0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x67,
	0x6d, 0x65, 0x6e, 0x74, 0x52, 0x07, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x51, 0x0a,
	0x0f, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x21, 0x0a, 0x0c, 0x61, 0x63, 0x6b, 0x5f, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x61, 0x63, 0x6b, 0x53, 0x65, 0x71, 0x75, 0x65,
	0x6e, 0x63, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x69, 0x73, 0x5f, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x69, 0x73, 0x43, 0x6c, 0x6f, 0x73, 0x65, 0x64,
	0x32, 0x83, 0x04, 0x0a, 0x10, 0x53, 0x65, 0x61, 0x77, 0x65, 0x65, 0x64, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x69, 0x6e, 0x67, 0x12, 0x63, 0x0a, 0x10, 0x46, 0x69, 0x6e, 0x64, 0x42, 0x72, 0x6f,
	0x6b, 0x65, 0x72, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x25, 0x2e, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x46, 0x69, 0x6e, 0x64, 0x42, 0x72, 0x6f,
	0x6b, 0x65, 0x72, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x26, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e,
	0x46, 0x69, 0x6e, 0x64, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x6f, 0x0a, 0x14, 0x41, 0x73,
	0x73, 0x69, 0x67, 0x6e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x72, 0x6f, 0x6b, 0x65,
	0x72, 0x73, 0x12, 0x29, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70,
	0x62, 0x2e, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x42,
	0x72, 0x6f, 0x6b, 0x65, 0x72, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x2a, 0x2e,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x41, 0x73, 0x73,
	0x69, 0x67, 0x6e, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x69, 0x0a, 0x12, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61, 0x74, 0x75,
	0x73, 0x12, 0x27, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62,
	0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x53, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x28, 0x2e, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x53,
	0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x60, 0x0a, 0x0f, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x42,
	0x72, 0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x6f, 0x61, 0x64, 0x12, 0x24, 0x2e, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x42, 0x72,
	0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x6f, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x25, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62, 0x2e, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x4c, 0x6f, 0x61, 0x64, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x4c, 0x0a, 0x07, 0x50, 0x75, 0x62, 0x6c,
	0x69, 0x73, 0x68, 0x12, 0x1c, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f,
	0x70, 0x62, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x1d, 0x2e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x69, 0x6e, 0x67, 0x5f, 0x70, 0x62,
	0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x28, 0x01, 0x30, 0x01, 0x42, 0x4e, 0x0a, 0x0c, 0x73, 0x65, 0x61, 0x77, 0x65, 0x65,
	0x64, 0x66, 0x73, 0x2e, 0x6d, 0x71, 0x42, 0x10, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x2c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x73, 0x65, 0x61, 0x77, 0x65, 0x65, 0x64, 0x66, 0x73, 0x2f, 0x73,
	0x65, 0x61, 0x77, 0x65, 0x65, 0x64, 0x66, 0x73, 0x2f, 0x77, 0x65, 0x65, 0x64, 0x2f, 0x70, 0x62,
	0x2f, 0x6d, 0x71, 0x5f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_mq_proto_rawDescOnce sync.Once
	file_mq_proto_rawDescData = file_mq_proto_rawDesc
)

func file_mq_proto_rawDescGZIP() []byte {
	file_mq_proto_rawDescOnce.Do(func() {
		file_mq_proto_rawDescData = protoimpl.X.CompressGZIP(file_mq_proto_rawDescData)
	})
	return file_mq_proto_rawDescData
}

var file_mq_proto_msgTypes = make([]protoimpl.MessageInfo, 14)
var file_mq_proto_goTypes = []interface{}{
	(*SegmentInfo)(nil),                  // 0: messaging_pb.SegmentInfo
	(*FindBrokerLeaderRequest)(nil),      // 1: messaging_pb.FindBrokerLeaderRequest
	(*FindBrokerLeaderResponse)(nil),     // 2: messaging_pb.FindBrokerLeaderResponse
	(*Partition)(nil),                    // 3: messaging_pb.Partition
	(*Segment)(nil),                      // 4: messaging_pb.Segment
	(*AssignSegmentBrokersRequest)(nil),  // 5: messaging_pb.AssignSegmentBrokersRequest
	(*AssignSegmentBrokersResponse)(nil), // 6: messaging_pb.AssignSegmentBrokersResponse
	(*CheckSegmentStatusRequest)(nil),    // 7: messaging_pb.CheckSegmentStatusRequest
	(*CheckSegmentStatusResponse)(nil),   // 8: messaging_pb.CheckSegmentStatusResponse
	(*CheckBrokerLoadRequest)(nil),       // 9: messaging_pb.CheckBrokerLoadRequest
	(*CheckBrokerLoadResponse)(nil),      // 10: messaging_pb.CheckBrokerLoadResponse
	(*PublishRequest)(nil),               // 11: messaging_pb.PublishRequest
	(*PublishResponse)(nil),              // 12: messaging_pb.PublishResponse
	(*PublishRequest_InitMessage)(nil),   // 13: messaging_pb.PublishRequest.InitMessage
}
var file_mq_proto_depIdxs = []int32{
	4,  // 0: messaging_pb.SegmentInfo.segment:type_name -> messaging_pb.Segment
	3,  // 1: messaging_pb.Segment.partition:type_name -> messaging_pb.Partition
	4,  // 2: messaging_pb.AssignSegmentBrokersRequest.segment:type_name -> messaging_pb.Segment
	4,  // 3: messaging_pb.CheckSegmentStatusRequest.segment:type_name -> messaging_pb.Segment
	13, // 4: messaging_pb.PublishRequest.init:type_name -> messaging_pb.PublishRequest.InitMessage
	4,  // 5: messaging_pb.PublishRequest.InitMessage.segment:type_name -> messaging_pb.Segment
	1,  // 6: messaging_pb.SeaweedMessaging.FindBrokerLeader:input_type -> messaging_pb.FindBrokerLeaderRequest
	5,  // 7: messaging_pb.SeaweedMessaging.AssignSegmentBrokers:input_type -> messaging_pb.AssignSegmentBrokersRequest
	7,  // 8: messaging_pb.SeaweedMessaging.CheckSegmentStatus:input_type -> messaging_pb.CheckSegmentStatusRequest
	9,  // 9: messaging_pb.SeaweedMessaging.CheckBrokerLoad:input_type -> messaging_pb.CheckBrokerLoadRequest
	11, // 10: messaging_pb.SeaweedMessaging.Publish:input_type -> messaging_pb.PublishRequest
	2,  // 11: messaging_pb.SeaweedMessaging.FindBrokerLeader:output_type -> messaging_pb.FindBrokerLeaderResponse
	6,  // 12: messaging_pb.SeaweedMessaging.AssignSegmentBrokers:output_type -> messaging_pb.AssignSegmentBrokersResponse
	8,  // 13: messaging_pb.SeaweedMessaging.CheckSegmentStatus:output_type -> messaging_pb.CheckSegmentStatusResponse
	10, // 14: messaging_pb.SeaweedMessaging.CheckBrokerLoad:output_type -> messaging_pb.CheckBrokerLoadResponse
	12, // 15: messaging_pb.SeaweedMessaging.Publish:output_type -> messaging_pb.PublishResponse
	11, // [11:16] is the sub-list for method output_type
	6,  // [6:11] is the sub-list for method input_type
	6,  // [6:6] is the sub-list for extension type_name
	6,  // [6:6] is the sub-list for extension extendee
	0,  // [0:6] is the sub-list for field type_name
}

func init() { file_mq_proto_init() }
func file_mq_proto_init() {
	if File_mq_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_mq_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SegmentInfo); i {
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
		file_mq_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FindBrokerLeaderRequest); i {
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
		file_mq_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FindBrokerLeaderResponse); i {
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
		file_mq_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Partition); i {
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
		file_mq_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Segment); i {
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
		file_mq_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AssignSegmentBrokersRequest); i {
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
		file_mq_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AssignSegmentBrokersResponse); i {
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
		file_mq_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CheckSegmentStatusRequest); i {
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
		file_mq_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CheckSegmentStatusResponse); i {
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
		file_mq_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CheckBrokerLoadRequest); i {
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
		file_mq_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CheckBrokerLoadResponse); i {
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
		file_mq_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishRequest); i {
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
		file_mq_proto_msgTypes[12].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishResponse); i {
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
		file_mq_proto_msgTypes[13].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishRequest_InitMessage); i {
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
			RawDescriptor: file_mq_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   14,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_mq_proto_goTypes,
		DependencyIndexes: file_mq_proto_depIdxs,
		MessageInfos:      file_mq_proto_msgTypes,
	}.Build()
	File_mq_proto = out.File
	file_mq_proto_rawDesc = nil
	file_mq_proto_goTypes = nil
	file_mq_proto_depIdxs = nil
}
