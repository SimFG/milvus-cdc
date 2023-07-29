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
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/server/model"
)

type CDCServerConfig struct {
	Address         string // like: "localhost:8080"
	MaxTaskNum      int
	MetaStoreConfig CDCMetaStoreConfig // cdc meta data save
	SourceConfig    MilvusSourceConfig // cdc source
	EnableReverse   bool
	ReverseMilvus   model.MilvusConnectParam
	CurrentMilvus   model.MilvusConnectParam
	MaxNameLength   int
}

type CDCMetaStoreConfig struct {
	StoreType      string
	EtcdEndpoints  []string
	MysqlSourceUrl string
	RootPath       string
}

type MilvusSourceConfig struct {
	EtcdAddress          []string
	EtcdRootPath         string
	EtcdMetaSubPath      string
	ReadChanLen          int
	DefaultPartitionName string
	Pulsar               config.PulsarConfig
	Kafka                config.KafkaConfig
}
