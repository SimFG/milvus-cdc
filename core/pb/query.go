/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pb

import "github.com/golang/protobuf/proto"

type CollectionLoadInfo struct {
	CollectionID         int64           `protobuf:"varint,1,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	ReleasedPartitions   []int64         `protobuf:"varint,2,rep,packed,name=released_partitions,json=releasedPartitions,proto3" json:"released_partitions,omitempty"`
	ReplicaNumber        int32           `protobuf:"varint,3,opt,name=replica_number,json=replicaNumber,proto3" json:"replica_number,omitempty"`
	Status               LoadStatus      `protobuf:"varint,4,opt,name=status,proto3,enum=milvus.proto.query.LoadStatus" json:"status,omitempty"`
	FieldIndexID         map[int64]int64 `protobuf:"bytes,5,rep,name=field_indexID,json=fieldIndexID,proto3" json:"field_indexID,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	LoadType             LoadType        `protobuf:"varint,6,opt,name=load_type,json=loadType,proto3,enum=milvus.proto.query.LoadType" json:"load_type,omitempty"`
	RecoverTimes         int32           `protobuf:"varint,7,opt,name=recover_times,json=recoverTimes,proto3" json:"recover_times,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *CollectionLoadInfo) Reset()         { *m = CollectionLoadInfo{} }
func (m *CollectionLoadInfo) String() string { return proto.CompactTextString(m) }
func (*CollectionLoadInfo) ProtoMessage()    {}

func (m *CollectionLoadInfo) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *CollectionLoadInfo) GetReleasedPartitions() []int64 {
	if m != nil {
		return m.ReleasedPartitions
	}
	return nil
}

func (m *CollectionLoadInfo) GetReplicaNumber() int32 {
	if m != nil {
		return m.ReplicaNumber
	}
	return 0
}

func (m *CollectionLoadInfo) GetStatus() LoadStatus {
	if m != nil {
		return m.Status
	}
	return LoadStatus_Invalid
}

func (m *CollectionLoadInfo) GetFieldIndexID() map[int64]int64 {
	if m != nil {
		return m.FieldIndexID
	}
	return nil
}

func (m *CollectionLoadInfo) GetLoadType() LoadType {
	if m != nil {
		return m.LoadType
	}
	return LoadType_UnKnownType
}

func (m *CollectionLoadInfo) GetRecoverTimes() int32 {
	if m != nil {
		return m.RecoverTimes
	}
	return 0
}

type LoadType int32

const (
	LoadType_UnKnownType    LoadType = 0
	LoadType_LoadPartition  LoadType = 1
	LoadType_LoadCollection LoadType = 2
)

var LoadType_name = map[int32]string{
	0: "UnKnownType",
	1: "LoadPartition",
	2: "LoadCollection",
}

var LoadType_value = map[string]int32{
	"UnKnownType":    0,
	"LoadPartition":  1,
	"LoadCollection": 2,
}

func (x LoadType) String() string {
	return proto.EnumName(LoadType_name, int32(x))
}

type LoadStatus int32

const (
	LoadStatus_Invalid LoadStatus = 0
	LoadStatus_Loading LoadStatus = 1
	LoadStatus_Loaded  LoadStatus = 2
)

var LoadStatus_name = map[int32]string{
	0: "Invalid",
	1: "Loading",
	2: "Loaded",
}

var LoadStatus_value = map[string]int32{
	"Invalid": 0,
	"Loading": 1,
	"Loaded":  2,
}

func (x LoadStatus) String() string {
	return proto.EnumName(LoadStatus_name, int32(x))
}
