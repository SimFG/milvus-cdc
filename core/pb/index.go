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

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type FieldIndex struct {
	IndexInfo            *IndexInfo `protobuf:"bytes,1,opt,name=index_info,json=indexInfo,proto3" json:"index_info,omitempty"`
	Deleted              bool       `protobuf:"varint,2,opt,name=deleted,proto3" json:"deleted,omitempty"`
	CreateTime           uint64     `protobuf:"varint,3,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *FieldIndex) Reset()         { *m = FieldIndex{} }
func (m *FieldIndex) String() string { return proto.CompactTextString(m) }
func (*FieldIndex) ProtoMessage()    {}

func (m *FieldIndex) GetIndexInfo() *IndexInfo {
	if m != nil {
		return m.IndexInfo
	}
	return nil
}

func (m *FieldIndex) GetDeleted() bool {
	if m != nil {
		return m.Deleted
	}
	return false
}

func (m *FieldIndex) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

type IndexInfo struct {
	CollectionID int64                    `protobuf:"varint,1,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	FieldID      int64                    `protobuf:"varint,2,opt,name=fieldID,proto3" json:"fieldID,omitempty"`
	IndexName    string                   `protobuf:"bytes,3,opt,name=index_name,json=indexName,proto3" json:"index_name,omitempty"`
	IndexID      int64                    `protobuf:"varint,4,opt,name=indexID,proto3" json:"indexID,omitempty"`
	TypeParams   []*commonpb.KeyValuePair `protobuf:"bytes,5,rep,name=type_params,json=typeParams,proto3" json:"type_params,omitempty"`
	IndexParams  []*commonpb.KeyValuePair `protobuf:"bytes,6,rep,name=index_params,json=indexParams,proto3" json:"index_params,omitempty"`
	// index build progress
	// The real-time statistics may not be expected due to the existence of the compaction mechanism.
	IndexedRows int64 `protobuf:"varint,7,opt,name=indexed_rows,json=indexedRows,proto3" json:"indexed_rows,omitempty"`
	TotalRows   int64 `protobuf:"varint,8,opt,name=total_rows,json=totalRows,proto3" json:"total_rows,omitempty"`
	// index state
	State                commonpb.IndexState      `protobuf:"varint,9,opt,name=state,proto3,enum=milvus.proto.common.IndexState" json:"state,omitempty"`
	IndexStateFailReason string                   `protobuf:"bytes,10,opt,name=index_state_fail_reason,json=indexStateFailReason,proto3" json:"index_state_fail_reason,omitempty"`
	IsAutoIndex          bool                     `protobuf:"varint,11,opt,name=is_auto_index,json=isAutoIndex,proto3" json:"is_auto_index,omitempty"`
	UserIndexParams      []*commonpb.KeyValuePair `protobuf:"bytes,12,rep,name=user_index_params,json=userIndexParams,proto3" json:"user_index_params,omitempty"`
	PendingIndexRows     int64                    `protobuf:"varint,13,opt,name=pending_index_rows,json=pendingIndexRows,proto3" json:"pending_index_rows,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *IndexInfo) Reset()         { *m = IndexInfo{} }
func (m *IndexInfo) String() string { return proto.CompactTextString(m) }
func (*IndexInfo) ProtoMessage()    {}

func (m *IndexInfo) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *IndexInfo) GetFieldID() int64 {
	if m != nil {
		return m.FieldID
	}
	return 0
}

func (m *IndexInfo) GetIndexName() string {
	if m != nil {
		return m.IndexName
	}
	return ""
}

func (m *IndexInfo) GetIndexID() int64 {
	if m != nil {
		return m.IndexID
	}
	return 0
}

func (m *IndexInfo) GetTypeParams() []*commonpb.KeyValuePair {
	if m != nil {
		return m.TypeParams
	}
	return nil
}

func (m *IndexInfo) GetIndexParams() []*commonpb.KeyValuePair {
	if m != nil {
		return m.IndexParams
	}
	return nil
}

func (m *IndexInfo) GetIndexedRows() int64 {
	if m != nil {
		return m.IndexedRows
	}
	return 0
}

func (m *IndexInfo) GetTotalRows() int64 {
	if m != nil {
		return m.TotalRows
	}
	return 0
}

func (m *IndexInfo) GetState() commonpb.IndexState {
	if m != nil {
		return m.State
	}
	return commonpb.IndexState_IndexStateNone
}

func (m *IndexInfo) GetIndexStateFailReason() string {
	if m != nil {
		return m.IndexStateFailReason
	}
	return ""
}

func (m *IndexInfo) GetIsAutoIndex() bool {
	if m != nil {
		return m.IsAutoIndex
	}
	return false
}

func (m *IndexInfo) GetUserIndexParams() []*commonpb.KeyValuePair {
	if m != nil {
		return m.UserIndexParams
	}
	return nil
}

func (m *IndexInfo) GetPendingIndexRows() int64 {
	if m != nil {
		return m.PendingIndexRows
	}
	return 0
}
