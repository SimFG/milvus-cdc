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

package reader

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.MetaOp = (*EtcdOp)(nil)

const (
	collectionPrefix = "root-coord/database/collection-info"
	partitionPrefix  = "root-coord/partitions"
	fieldPrefix      = "root-coord/fields"
	databasePrefix   = "root-coord/database/db-info"
)

type EtcdOp struct {
	endpoints            []string
	rootPath             string
	metaSubPath          string
	defaultPartitionName string
	etcdClient           *clientv3.Client

	// don't use the name to get id, because the same name may have different id when an object is deleted and recreated
	collectionID2Name util.Map[int64, string]
	collectionID2DBID util.Map[int64, int64]
	// don't use the name to get id
	dbID2Name util.Map[int64, string]

	watchCollectionOnce sync.Once
	watchPartitionOnce  sync.Once
	retryOptions        []retry.Option

	// task id -> api.CollectionFilter
	subscribeCollectionEvent util.Map[string, api.CollectionEventConsumer]
	subscribePartitionEvent  util.Map[string, api.PartitionEventConsumer]
}

func NewEtcdOp(endpoints []string,
	rootPath, metaPath, defaultPartitionName string,
) (api.MetaOp, error) {
	etcdOp := &EtcdOp{
		endpoints:            endpoints,
		rootPath:             rootPath,
		metaSubPath:          metaPath,
		defaultPartitionName: defaultPartitionName,
		retryOptions:         util.GetRetryOptionsFor25s(),
	}

	// set default value
	if len(endpoints) == 0 {
		etcdOp.endpoints = []string{"127.0.0.1:2379"}
	}
	if rootPath == "" {
		etcdOp.rootPath = "by-dev"
	}
	if metaPath == "" {
		etcdOp.metaSubPath = "meta"
	}
	if defaultPartitionName == "" {
		etcdOp.defaultPartitionName = "_default"
	}

	var err error
	log := log.With(zap.Strings("endpoints", endpoints))
	etcdOp.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   etcdOp.endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Warn("create etcd client failed", zap.Error(err))
		return nil, err
	}
	// check etcd status
	timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = etcdOp.etcdClient.Status(timeCtx, etcdOp.endpoints[0])
	if err != nil {
		log.Warn("etcd status check failed", zap.Error(err))
		return nil, err
	}
	log.Debug("success to create etcd client")

	return etcdOp, nil
}

func (e *EtcdOp) collectionPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, collectionPrefix)
}

func (e *EtcdOp) partitionPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, partitionPrefix)
}

func (e *EtcdOp) fieldPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, fieldPrefix)
}

func (e *EtcdOp) databasePrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, databasePrefix)
}

func (e *EtcdOp) WatchCollection(ctx context.Context, filter api.CollectionFilter) {
	e.watchCollectionOnce.Do(func() {
		watchChan := e.etcdClient.Watch(ctx, e.collectionPrefix()+"/", clientv3.WithPrefix())
		go func() {
			for {
				select {
				case watchResp, ok := <-watchChan:
					if !ok {
						log.Info("etcd watch collection channel closed")
						return
					}
					for _, event := range watchResp.Events {
						if event.Type != clientv3.EventTypePut {
							log.Debug("collection watch event type is not put", zap.String("event type", event.Type.String()))
							continue
						}
						collectionKey := util.ToString(event.Kv.Key)
						info := &pb.CollectionInfo{}
						err := proto.Unmarshal(event.Kv.Value, info)
						if err != nil {
							log.Warn("fail to unmarshal the collection info", zap.String("key", collectionKey), zap.String("value", util.Base64Encode(event.Kv.Value)), zap.Error(err))
							continue
						}
						if info.State != pb.CollectionState_CollectionCreated {
							log.Info("the collection state is not created", zap.String("key", collectionKey), zap.String("state", info.State.String()))
							continue
						}
						if filter != nil && filter(info) {
							log.Info("the collection is filtered in the watch process", zap.String("key", collectionKey))
							continue
						}

						err = retry.Do(ctx, func() error {
							return e.fillCollectionField(info)
						}, e.retryOptions...)
						if err != nil {
							log.Warn("fail to fill collection field in the watch process", zap.String("key", collectionKey), zap.Error(err))
							continue
						}
						e.collectionID2Name.Store(info.ID, info.Schema.Name)
						if databaseID := e.getDatabaseIDFromCollectionKey(collectionKey); databaseID != 0 {
							e.collectionID2DBID.Store(info.ID, databaseID)
						}
						e.subscribeCollectionEvent.Range(func(key string, value api.CollectionEventConsumer) bool {
							if value != nil && value(info) {
								log.Info("the collection has been consumed", zap.Int64("collection_id", info.ID), zap.String("task_id", key))
								return false
							}
							return true
						})
					}
				case <-ctx.Done():
					log.Info("watch collection context done")
					return
				}
			}
		}()
	})
}

func (e *EtcdOp) SubscribeCollectionEvent(taskID string, consumer api.CollectionEventConsumer) {
	e.subscribeCollectionEvent.Store(taskID, consumer)
}

func (e *EtcdOp) SubscribePartitionEvent(taskID string, consumer api.PartitionEventConsumer) {
	e.subscribePartitionEvent.Store(taskID, consumer)
}

func (e *EtcdOp) UnsubscribeEvent(taskID string, eventType api.WatchEventType) {
	switch eventType {
	case api.CollectionEventType:
		e.subscribeCollectionEvent.Delete(taskID)
	case api.PartitionEventType:
		e.subscribePartitionEvent.Delete(taskID)
	default:
		log.Warn("unknown event type", zap.String("taskID", taskID), zap.Any("eventType", eventType))
	}
}

func (e *EtcdOp) WatchPartition(ctx context.Context, filter api.PartitionFilter) {
	e.watchPartitionOnce.Do(func() {
		watchChan := e.etcdClient.Watch(ctx, e.partitionPrefix()+"/", clientv3.WithPrefix())
		go func() {
			for {
				select {
				case watchResp, ok := <-watchChan:
					if !ok {
						log.Info("etcd watch partition channel closed")
						return
					}
					for _, event := range watchResp.Events {
						if event.Type != clientv3.EventTypePut {
							log.Debug("partition watch event type is not put", zap.String("event type", event.Type.String()))
							continue
						}
						partitionKey := util.ToString(event.Kv.Key)
						info := &pb.PartitionInfo{}
						err := proto.Unmarshal(event.Kv.Value, info)
						if err != nil {
							log.Warn("fail to unmarshal the partition info", zap.String("key", partitionKey), zap.Error(err))
							continue
						}
						if info.State != pb.PartitionState_PartitionCreated ||
							info.PartitionName == e.defaultPartitionName {
							log.Debug("partition state is not created or partition name is default", zap.String("partition name", info.PartitionName), zap.Any("state", info.State))
							continue
						}
						if filter != nil && filter(info) {
							log.Info("partition filter", zap.String("partition name", info.PartitionName))
							continue
						}

						log.Debug("get a new partition in the watch process", zap.String("key", partitionKey))
						e.subscribePartitionEvent.Range(func(key string, value api.PartitionEventConsumer) bool {
							if value != nil && value(info) {
								log.Info("the partition has been consumed", zap.String("key", partitionKey), zap.String("task_id", key))
								return false
							}
							return true
						})
					}
				case <-ctx.Done():
					log.Info("watch partition context done")
					return
				}
			}
		}()
	})
}

func (e *EtcdOp) getCollectionIDFromPartitionKey(key string) int64 {
	subString := strings.Split(key[len(e.partitionPrefix())+1:], "/")
	if len(subString) != 2 {
		log.Warn("the key is invalid", zap.String("key", key), zap.Strings("sub", subString))
		return 0
	}
	id, err := strconv.ParseInt(subString[0], 10, 64)
	if err != nil {
		log.Warn("fail to parse the collection id", zap.String("id", subString[0]), zap.Error(err))
		return 0
	}
	return id
}

func (e *EtcdOp) getDatabaseIDFromCollectionKey(key string) int64 {
	subString := strings.Split(key[len(e.collectionPrefix())+1:], "/")
	if len(subString) != 2 {
		log.Warn("the key is invalid", zap.String("key", key), zap.Strings("sub", subString))
		return 0
	}
	id, err := strconv.ParseInt(subString[0], 10, 64)
	if err != nil {
		log.Warn("fail to parse the database id", zap.String("id", subString[0]), zap.Error(err))
		return 0
	}
	return id
}

func (e *EtcdOp) getDatabases(ctx context.Context) ([]model.DatabaseInfo, error) {
	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.databasePrefix(), clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all databases", zap.Error(err))
		return nil, err
	}
	var databases []model.DatabaseInfo
	for _, kv := range resp.Kvs {
		info := &pb.DatabaseInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal database info", zap.Error(err))
			return nil, err
		}
		databases = append(databases, model.DatabaseInfo{
			ID:   info.Id,
			Name: info.Name,
		})
		e.dbID2Name.Store(info.Id, info.Name)
	}
	return databases, nil
}

func (e *EtcdOp) getCollectionNameByID(ctx context.Context, collectionID int64) string {
	var (
		resp     *clientv3.GetResponse
		database model.DatabaseInfo
		err      error
	)

	databases, err := e.getDatabases(ctx)
	if err != nil {
		log.Warn("fail to get all databases", zap.Error(err))
		return ""
	}

	for _, database = range databases {
		key := path.Join(e.collectionPrefix(), strconv.FormatInt(database.ID, 10), strconv.FormatInt(collectionID, 10))
		resp, err = util.EtcdGetWithContext(ctx, e.etcdClient, key)
		if err != nil {
			log.Warn("fail to get the collection data", zap.Int64("collection_id", collectionID), zap.Error(err))
			return ""
		}
		if len(resp.Kvs) == 0 {
			continue
		}
	}
	if resp == nil {
		log.Warn("there is no database")
		return ""
	}
	if len(resp.Kvs) == 0 {
		log.Warn("the collection isn't existed", zap.Int64("collection_id", collectionID))
		return ""
	}

	info := &pb.CollectionInfo{}
	err = proto.Unmarshal(resp.Kvs[0].Value, info)
	if err != nil {
		log.Warn("fail to unmarshal collection info, maybe it's a deleted collection",
			zap.Int64("collection_id", collectionID),
			zap.String("value", util.Base64Encode(resp.Kvs[0].Value)),
			zap.Error(err))
		return ""
	}
	collectionName := info.Schema.GetName()
	e.collectionID2Name.Store(collectionID, collectionName)
	e.collectionID2DBID.Store(collectionID, database.ID)

	return collectionName
}

func (e *EtcdOp) GetAllCollection(ctx context.Context, filter api.CollectionFilter) ([]*pb.CollectionInfo, error) {
	_, _ = e.getDatabases(ctx)

	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.collectionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all collection data", zap.Error(err))
		return nil, err
	}
	var existedCollectionInfos []*pb.CollectionInfo

	for _, kv := range resp.Kvs {
		info := &pb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Info("fail to unmarshal collection info, maybe it's a deleted collection", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			continue
		}
		if info.State != pb.CollectionState_CollectionCreated {
			log.Info("not created collection", zap.String("key", util.ToString(kv.Key)))
			continue
		}
		if filter != nil && filter(info) {
			log.Info("the collection info is filtered", zap.String("key", util.ToString(kv.Key)))
			continue
		}
		err = e.fillCollectionField(info)
		if err != nil {
			log.Warn("fail to fill collection field", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			continue
		}
		e.collectionID2Name.Store(info.ID, info.Schema.Name)
		if databaseID := e.getDatabaseIDFromCollectionKey(util.ToString(kv.Key)); databaseID != 0 {
			e.collectionID2DBID.Store(info.ID, databaseID)
		}
		existedCollectionInfos = append(existedCollectionInfos, info)
	}
	return existedCollectionInfos, nil
}

func (e *EtcdOp) fillCollectionField(info *pb.CollectionInfo) error {
	prefix := path.Join(e.fieldPrefix(), strconv.FormatInt(info.ID, 10)) + "/"
	resp, err := util.EtcdGetWithContext(context.Background(), e.etcdClient, prefix, clientv3.WithPrefix())
	log := log.With(zap.String("prefix", prefix))
	if err != nil {
		log.Warn("fail to get the collection field data", zap.Error(err))
		return err
	}
	if len(resp.Kvs) == 0 {
		msg := "not found the collection field data"
		log.Warn(msg)
		return errors.New(msg)
	}
	var fields []*schemapb.FieldSchema
	for _, kv := range resp.Kvs {
		field := &schemapb.FieldSchema{}
		err = proto.Unmarshal(kv.Value, field)
		if err != nil {
			log.Warn("fail to unmarshal filed schema info", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return err
		}
		if field.Name == common.MetaFieldName {
			info.Schema.EnableDynamicField = true
			continue
		}
		// if the field id is less than 100, it is a system field, skip it.
		if field.FieldID < 100 {
			continue
		}
		fields = append(fields, field)
	}
	info.Schema.Fields = fields
	return nil
}

func (e *EtcdOp) GetCollectionNameByID(ctx context.Context, id int64) string {
	collectionName, ok := e.collectionID2Name.Load(id)
	if !ok {
		collectionName = e.getCollectionNameByID(ctx, id)
		if collectionName == "" {
			log.Warn("not found the collection", zap.Int64("collection_id", id))
		}
	}
	return collectionName
}

func (e *EtcdOp) GetDatabaseInfoForCollection(ctx context.Context, id int64) model.DatabaseInfo {
	dbID, _ := e.collectionID2DBID.Load(id)
	dbName, _ := e.dbID2Name.Load(dbID)
	if dbName != "" {
		return model.DatabaseInfo{
			ID:   dbID,
			Name: dbName,
		}
	}

	// it will update all database info and this collection info
	_ = e.getCollectionNameByID(ctx, id)
	dbID, _ = e.collectionID2DBID.Load(id)
	dbName, _ = e.dbID2Name.Load(dbID)
	return model.DatabaseInfo{
		ID:   dbID,
		Name: dbName,
	}
}

func (e *EtcdOp) GetAllPartition(ctx context.Context, filter api.PartitionFilter) ([]*pb.PartitionInfo, error) {
	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.partitionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all partition data", zap.Error(err))
		return nil, err
	}
	var existedPartitionInfos []*pb.PartitionInfo
	for _, kv := range resp.Kvs {
		info := &pb.PartitionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal partition info", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			continue
		}
		if info.State != pb.PartitionState_PartitionCreated || info.PartitionName == e.defaultPartitionName {
			log.Info("not created partition", zap.String("key", util.ToString(kv.Key)), zap.String("partition_name", info.PartitionName))
			continue
		}
		if filter != nil && filter(info) {
			log.Info("the partition info is filtered", zap.String("key", util.ToString(kv.Key)), zap.String("partition_name", info.PartitionName))
			continue
		}
		existedPartitionInfos = append(existedPartitionInfos, info)
	}
	return existedPartitionInfos, nil
}