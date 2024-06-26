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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-sdk-go/v2/client"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.TargetAPI = (*TargetClient)(nil)

type TargetClient struct {
	client client.Client
	config TargetConfig
}

type TargetConfig struct {
	Address    string
	Username   string
	Password   string
	APIKey     string
	EnableTLS  bool
	DialConfig util.DialConfig
}

func NewTarget(ctx context.Context, config TargetConfig) (api.TargetAPI, error) {
	targetClient := &TargetClient{
		config: config,
	}

	_, err := targetClient.GetMilvus(ctx, "")
	if err != nil {
		log.Warn("fail to new target client", zap.String("address", config.Address), zap.Error(err))
		return nil, err
	}
	return targetClient, nil
}

func (t *TargetClient) GetMilvus(ctx context.Context, databaseName string) (client.Client, error) {
	apiKey := t.config.APIKey
	if apiKey == "" {
		apiKey = util.GetAPIKey(t.config.Username, t.config.Password)
	}
	milvusClient, err := util.GetMilvusClientManager().GetMilvusClient(ctx, t.config.Address, apiKey, databaseName, t.config.EnableTLS, t.config.DialConfig)
	if err != nil {
		return nil, err
	}
	return milvusClient, nil
}

func (t *TargetClient) GetCollectionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}
	milvus, err := t.GetMilvus(ctx, databaseName)
	if err != nil {
		log.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	collection, err := milvus.DescribeCollection(ctx, collectionName)
	if err != nil {
		log.Warn("fail to describe collection", zap.Error(err))
		return nil, err
	}
	collectionInfo.DatabaseName = databaseName
	collectionInfo.CollectionID = collection.ID
	collectionInfo.CollectionName = collectionName
	collectionInfo.PChannels = collection.PhysicalChannels
	collectionInfo.VChannels = collection.VirtualChannels

	tmpCollectionInfo, err := t.GetPartitionInfo(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get partition info", zap.Error(err))
		return nil, err
	}
	collectionInfo.Partitions = tmpCollectionInfo.Partitions
	return collectionInfo, nil
}

func (t *TargetClient) GetPartitionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}
	milvus, err := t.GetMilvus(ctx, databaseName)
	if err != nil {
		log.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	partition, err := milvus.ShowPartitions(ctx, collectionName)
	if err != nil || len(partition) == 0 {
		log.Warn("failed to show partitions", zap.Error(err))
		return nil, errors.New("fail to show the partitions")
	}
	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	collectionInfo.Partitions = partitionInfo
	return collectionInfo, nil
}

func (t *TargetClient) GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error) {
	if !IsDroppedObject(databaseName) {
		return databaseName, nil
	}
	dbLog := log.With(zap.String("collection", collectionName), zap.String("database", databaseName))
	milvus, err := t.GetMilvus(ctx, "")
	if err != nil {
		dbLog.Warn("fail to get milvus client", zap.String("database", databaseName), zap.Error(err))
		return "", err
	}
	databaseNames, err := milvus.ListDatabases(ctx)
	if err != nil {
		dbLog.Warn("fail to list databases", zap.String("database", databaseName), zap.Error(err))
		return "", err
	}
	for _, dbName := range databaseNames {
		dbMilvus, err := t.GetMilvus(ctx, dbName.Name)
		if err != nil {
			dbLog.Warn("fail to get milvus client", zap.String("connect_db", dbName.Name), zap.Error(err))
			return "", err
		}
		collections, err := dbMilvus.ListCollections(ctx)
		if err != nil {
			dbLog.Warn("fail to list collections", zap.String("connect_db", dbName.Name), zap.Error(err))
			return "", err
		}
		for _, collection := range collections {
			if collection.Name == collectionName {
				return dbName.Name, nil
			}
		}
	}
	dbLog.Warn("not found the database", zap.Any("databases", databaseNames))
	return "", util.NotFoundDatabase
}
