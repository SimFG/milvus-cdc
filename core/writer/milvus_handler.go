// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

import (
	"context"
	"errors"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
	"go.uber.org/zap"
)

type MilvusDataHandler struct {
	DefaultDataHandler

	address         string
	username        string
	password        string
	enableTLS       bool
	ignorePartition bool // sometimes the has partition api is a deny api
	connectTimeout  int

	factory MilvusClientFactory
	// TODO support db
	milvus MilvusClientAPI
}

// NewMilvusDataHandler options must include AddressOption
func NewMilvusDataHandler(options ...config.Option[*MilvusDataHandler]) (*MilvusDataHandler, error) {
	handler := &MilvusDataHandler{
		connectTimeout: 5,
		factory:        NewDefaultMilvusClientFactory(),
	}
	for _, option := range options {
		option.Apply(handler)
	}
	if handler.address == "" {
		return nil, errors.New("empty milvus address")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	switch {
	case handler.username != "" && handler.enableTLS:
		handler.milvus, err = handler.factory.NewGrpcClientWithTLSAuth(timeoutContext,
			handler.address, handler.username, handler.password)
	case handler.username != "":
		handler.milvus, err = handler.factory.NewGrpcClientWithAuth(timeoutContext,
			handler.address, handler.username, handler.password)
	default:
		handler.milvus, err = handler.factory.NewGrpcClient(timeoutContext, handler.address)
	}
	if err != nil {
		log.Warn("fail to new the milvus client", zap.Error(err))
		return nil, err
	}
	return handler, nil
}

func (m *MilvusDataHandler) CreateCollection(ctx context.Context, param *CreateCollectionParam) error {
	var options []client.CreateCollectionOption
	for _, property := range param.Properties {
		options = append(options, client.WithCollectionProperty(property.GetKey(), property.GetValue()))
	}
	options = append(options, client.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel)))
	return m.milvus.CreateCollection(ctx, param.Schema, param.ShardsNum, options...)
}

func (m *MilvusDataHandler) DropCollection(ctx context.Context, param *DropCollectionParam) error {
	return m.milvus.DropCollection(ctx, param.CollectionName)
}

func (m *MilvusDataHandler) Insert(ctx context.Context, param *InsertParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		partitionName = ""
	}
	_, err := m.milvus.Insert(ctx, param.CollectionName, partitionName, param.Columns...)
	return err
}

func (m *MilvusDataHandler) Delete(ctx context.Context, param *DeleteParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		partitionName = ""
	}
	return m.milvus.DeleteByPks(ctx, param.CollectionName, partitionName, param.Column)
}

func (m *MilvusDataHandler) CreatePartition(ctx context.Context, param *CreatePartitionParam) error {
	if m.ignorePartition {
		return nil
	}
	return m.milvus.CreatePartition(ctx, param.CollectionName, param.PartitionName)
}

func (m *MilvusDataHandler) DropPartition(ctx context.Context, param *DropPartitionParam) error {
	if m.ignorePartition {
		return nil
	}
	return m.milvus.DropPartition(ctx, param.CollectionName, param.PartitionName)
}

func (m *MilvusDataHandler) CreateIndex(ctx context.Context, param *CreateIndexParam) error {
	indexEntity := entity.NewGenericIndex(param.IndexName, "", util.ConvertKVPairToMap(param.ExtraParams))
	return m.milvus.CreateIndex(ctx, param.CollectionName, param.FieldName, indexEntity, true, client.WithIndexName(param.IndexName))
}

func (m *MilvusDataHandler) DropIndex(ctx context.Context, param *DropIndexParam) error {
	return m.milvus.DropIndex(ctx, param.CollectionName, param.FieldName, client.WithIndexName(param.IndexName))
}

func (m *MilvusDataHandler) LoadCollection(ctx context.Context, param *LoadCollectionParam) error {
	// TODO resource group
	//return m.milvus.LoadCollection(ctx, param.CollectionName, true, client.WithReplicaNumber(param.ReplicaNumber), client.WithResourceGroups(param.ResourceGroups))
	return m.milvus.LoadCollection(ctx, param.CollectionName, true, client.WithReplicaNumber(param.ReplicaNumber))
}

func (m *MilvusDataHandler) ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error {
	return m.milvus.ReleaseCollection(ctx, param.CollectionName)
}

func (m *MilvusDataHandler) CreateDatabase(ctx context.Context, param *CreateDataBaseParam) error {
	return m.milvus.CreateDatabase(ctx, param.DbName)
}

func (m *MilvusDataHandler) DropDatabase(ctx context.Context, param *DropDataBaseParam) error {
	return m.milvus.DropDatabase(ctx, param.DbName)
}
