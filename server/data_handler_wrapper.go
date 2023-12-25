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

package server

import (
	"context"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/core/writer"
	"github.com/zilliztech/milvus-cdc/server/metrics"
)

type DataHandlerWrapper struct {
	writer.DefaultDataHandler
	taskID  string
	handler writer.CDCDataHandler
}

func NewDataHandlerWrapper(taskID string, handler writer.CDCDataHandler) writer.CDCDataHandler {
	return &DataHandlerWrapper{
		taskID:  taskID,
		handler: handler,
	}
}

func (d *DataHandlerWrapper) metric(collectionName string, apiType string, isErr bool) {
	if isErr {
		metrics.APIExecuteCountVec.WithLabelValues(d.taskID, collectionName, apiType, metrics.FailStatusLabel).Inc()
		return
	}
	metrics.APIExecuteCountVec.WithLabelValues(d.taskID, collectionName, apiType, metrics.SuccessStatusLabel).Inc()
}

func (d *DataHandlerWrapper) CreateCollection(ctx context.Context, param *writer.CreateCollectionParam) (err error) {
	defer func() {
		d.metric(param.Schema.CollectionName, "CreateCollection", err != nil)
	}()
	err = d.handler.CreateCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) DropCollection(ctx context.Context, param *writer.DropCollectionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "DropCollection", err != nil)
	}()
	err = d.handler.DropCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) Insert(ctx context.Context, param *writer.InsertParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "Insert", err != nil)
		logInfos := []zap.Field{
			zap.String("collection_name", param.CollectionName),
			zap.String("partition_name", param.PartitionName),
			zap.Int("data_count", GetDataLen(param.Columns)),
			zap.String("task_id", d.taskID),
			zap.Error(err),
		}
		for _, column := range param.Columns {
			if p, ok := column.(*entity.ColumnInt64); ok {
				logInfos = append(logInfos, zap.Int64s(p.Name(), p.Data()))
			}
		}
		log.Info("insert done", logInfos...)
	}()
	err = d.handler.Insert(ctx, param)
	return
}

func GetDataLen(columns []entity.Column) int {
	if len(columns) == 0 {
		return -1
	}
	return columns[0].Len()
}

func (d *DataHandlerWrapper) Delete(ctx context.Context, param *writer.DeleteParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "Delete", err != nil)
		pks := zap.String("empty", "unknown")
		if p, ok := param.Column.(*entity.ColumnInt64); ok {
			pks = zap.Int64s("pks", p.Data())
		}
		if p, ok := param.Column.(*entity.ColumnVarChar); ok {
			pks = zap.Strings("pks", p.Data())
		}

		log.Info("delete done",
			zap.String("collection_name", param.CollectionName),
			zap.String("partition_name", param.PartitionName),
			zap.Int("data_count", param.Column.Len()),
			zap.String("task_id", d.taskID),
			pks,
			zap.Error(err))
	}()
	err = d.handler.Delete(ctx, param)
	return
}

func (d *DataHandlerWrapper) CreatePartition(ctx context.Context, param *writer.CreatePartitionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "CreatePartition", err != nil)
	}()
	err = d.handler.CreatePartition(ctx, param)
	return
}

func (d *DataHandlerWrapper) DropPartition(ctx context.Context, param *writer.DropPartitionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "DropPartition", err != nil)
	}()
	err = d.handler.DropPartition(ctx, param)
	return
}

func (d *DataHandlerWrapper) CreateIndex(ctx context.Context, param *writer.CreateIndexParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "CreateIndex", err != nil)
	}()
	err = d.handler.CreateIndex(ctx, param)
	return
}

func (d *DataHandlerWrapper) DropIndex(ctx context.Context, param *writer.DropIndexParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "DropIndex", err != nil)
	}()
	err = d.handler.DropIndex(ctx, param)
	return
}

func (d *DataHandlerWrapper) LoadCollection(ctx context.Context, param *writer.LoadCollectionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "LoadCollection", err != nil)
	}()
	err = d.handler.LoadCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) ReleaseCollection(ctx context.Context, param *writer.ReleaseCollectionParam) (err error) {
	defer func() {
		d.metric(param.CollectionName, "ReleaseCollection", err != nil)
	}()
	err = d.handler.ReleaseCollection(ctx, param)
	return
}

func (d *DataHandlerWrapper) CreateDatabase(ctx context.Context, param *writer.CreateDataBaseParam) (err error) {
	defer func() {
		d.metric(util.RPCRequestCollectionName, "CreateDatabase", err != nil)
	}()
	err = d.handler.CreateDatabase(ctx, param)
	return
}

func (d *DataHandlerWrapper) DropDatabase(ctx context.Context, param *writer.DropDataBaseParam) (err error) {
	defer func() {
		d.metric(util.RPCRequestCollectionName, "DropDatabase", err != nil)
	}()
	err = d.handler.DropDatabase(ctx, param)
	return
}