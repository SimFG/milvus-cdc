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
	"github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"go.uber.org/zap"
)

type ReaderMonitor struct {
	reader.DefaultMonitor
	taskID string
	log    *zap.Logger
}

func NewReaderMonitor(taskID string) *ReaderMonitor {
	return &ReaderMonitor{
		taskID: taskID,
		log:    log.With(zap.String("task_id", taskID)),
	}
}

func (s *ReaderMonitor) OnFailUnKnowCollection(key string, err error) {
	s.log.Warn("fail unknown collection", zap.String("key", key), zap.Error(err))
	metrics.ReaderFailCountVec.WithLabelValues(s.taskID, metrics.ReadFailUnknown).Inc()
}

func (s *ReaderMonitor) OnFailGetCollectionInfo(collectionID int64, collectionName string, err error) {
	s.log.Warn("fail to get collection info", zap.Int64("id", collectionID),
		zap.String("name", collectionName), zap.Error(err))
	metrics.ReaderFailCountVec.WithLabelValues(s.taskID, metrics.ReadFailGetCollectionInfo).Inc()
}

func (s *ReaderMonitor) OnFailReadStream(collectionID int64, collectionName string, vchannel string, err error) {
	s.log.Warn("fail to read stream data", zap.Int64("id", collectionID),
		zap.String("name", collectionName), zap.String("channel", vchannel), zap.Error(err))
	metrics.ReaderFailCountVec.WithLabelValues(s.taskID, metrics.ReadFailReadStream).Inc()
	metrics.StreamingCollectionCountVec.WithLabelValues(s.taskID, metrics.FailStatusLabel).Inc()
}

func (s *ReaderMonitor) OnSuccessGetACollectionInfo(collectionID int64, collectionName string) {
	s.log.Info("success to get a collection info",
		zap.Int64("id", collectionID), zap.String("name", collectionName))
	metrics.StreamingCollectionCountVec.WithLabelValues(s.taskID, metrics.SuccessStatusLabel).Inc()
}

func (s *ReaderMonitor) WatchChanClosed() {
	s.log.Info("watch chan closed")
}

func (s *ReaderMonitor) OnFilterReadMsg(msgType string) {
	s.log.Info("filter msg", zap.String("msg_type", msgType))
}
