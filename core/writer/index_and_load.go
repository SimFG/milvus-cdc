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

package writer

import (
	"context"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

const (
	fieldPrefix    = "meta/root-coord/fields"
	indexPrefix    = "meta/field-index"
	loadInfoPrefix = "meta/querycoord-collection-loadinfo"
)

type LoadAndIndexForNewCollection struct {
	etcdConfig config.MilvusEtcdConfig
	etcdClient *clientv3.Client
}

// NewLoadAndIndexForNewCollection, only used when the collection will be quickly created index and load after it's created
func NewLoadAndIndexForNewCollection(etcdConfig config.MilvusEtcdConfig) *LoadAndIndexForNewCollection {
	l := &LoadAndIndexForNewCollection{
		etcdConfig: etcdConfig,
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdConfig.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Warn("create etcd client failed for load and index tool", zap.Error(err))
		return nil
	}
	l.etcdClient = client
	return l
}

func (l *LoadAndIndexForNewCollection) GetLoadKey(collectionID int64) string {
	return l.etcdConfig.RootPath + "/" + loadInfoPrefix + "/" + strconv.FormatInt(collectionID, 10)
}

func (l *LoadAndIndexForNewCollection) GetFieldKey(collectionID, fieldID int64) string {
	return l.etcdConfig.RootPath + "/" + fieldPrefix + "/" + strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(fieldID, 10)
}

func (l *LoadAndIndexForNewCollection) GetIndexPrefix(collectionID int64) string {
	return l.etcdConfig.RootPath + "/" + indexPrefix + "/" + strconv.FormatInt(collectionID, 10)
}

func (l *LoadAndIndexForNewCollection) Execute(id int64, name string, indexFunc func(*CreateIndexParam), loadFunc func(*LoadCollectionParam)) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	patchLog := log.With(zap.Int64("collectionID", id), zap.String("collectionName", name))
	watchChan := l.etcdClient.Watch(ctx, l.GetLoadKey(id))
	select {
	case watchResp, ok := <-watchChan:
		if !ok {
			patchLog.Info("load and index tool watch channel closed")
			return
		}
		for _, event := range watchResp.Events {
			if clientv3.EventTypePut != event.Type {
				continue
			}

			info := &pb.CollectionLoadInfo{}
			err := proto.Unmarshal(event.Kv.Value, info)
			if err != nil {
				patchLog.Warn("fail to unmarshal load info", zap.Error(err))
				return
			}

			l.Index(id, name, indexFunc)

			loadFunc(&LoadCollectionParam{
				LoadCollectionRequest: milvuspb.LoadCollectionRequest{
					CollectionName: name,
					ReplicaNumber:  info.GetReplicaNumber(),
				},
			})
			log.Info("load collection done")
			return
		}
	case <-ctx.Done():
		patchLog.Info("load and index tool context done")
	}
}

func (l *LoadAndIndexForNewCollection) Index(id int64, name string, indexFunc func(*CreateIndexParam)) {
	ctx := context.Background()
	patchLog := log.With(zap.Int64("collectionID", id), zap.String("collectionName", name))

	getResp, err := l.etcdClient.Get(ctx, l.GetIndexPrefix(id), clientv3.WithPrefix())
	if err != nil {
		patchLog.Warn("fail to get index prefix", zap.Error(err))
		return
	}
	if len(getResp.Kvs) == 0 {
		patchLog.Warn("no index prefix")
		return
	}
	for _, kv := range getResp.Kvs {
		fieldIndex := &pb.FieldIndex{}
		err := proto.Unmarshal(kv.Value, fieldIndex)
		if err != nil {
			patchLog.Warn("fail to unmarshal index info", zap.Error(err))
			return
		}
		indexInfo := fieldIndex.GetIndexInfo()
		fieldResp, err := l.etcdClient.Get(ctx, l.GetFieldKey(id, indexInfo.GetFieldID()))
		if err != nil {
			patchLog.Warn("fail to get field", zap.Error(err))
			return
		}
		if len(fieldResp.Kvs) == 0 {
			patchLog.Warn("no field", zap.Int64("fieldID", indexInfo.GetFieldID()))
			return
		}
		field := &schemapb.FieldSchema{}
		err = proto.Unmarshal(fieldResp.Kvs[0].Value, field)
		if err != nil {
			patchLog.Warn("fail to unmarshal field", zap.Error(err))
			return
		}
		indexFunc(&CreateIndexParam{
			CreateIndexRequest: milvuspb.CreateIndexRequest{
				CollectionName: name,
				FieldName:      field.GetName(),
				IndexName:      indexInfo.GetIndexName(),
				ExtraParams:    indexInfo.GetUserIndexParams(),
			},
		})
		log.Info("create index done", zap.String("fieldName", field.GetName()), zap.String("indexName", indexInfo.GetIndexName()))
	}
}
