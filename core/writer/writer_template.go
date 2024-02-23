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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var (
	log              = util.Log
	DynamicFieldName = "$meta"
)

type BufferConfig struct {
	Period time.Duration
	Size   int64
}

var DefaultBufferConfig = BufferConfig{
	Period: 1 * time.Minute,
	Size:   1024 * 1024,
}

var NoBufferConfig = BufferConfig{
	Period: 0,
	Size:   -1,
}

type CDCWriterTemplate struct {
	DefaultWriter

	handler    CDCDataHandler
	errProtect *ErrorProtect
	funcMap    map[msgstream.MsgType]func(context.Context, *model.CDCData, WriteCallback)

	bufferConfig             BufferConfig
	bufferLock               sync.Mutex
	currentBufferSize        int64
	bufferOps                []BufferOp
	bufferUpdatePositionFunc NotifyCollectionPositionChangeFunc
	bufferOpsChan            chan []BufferOp

	bufferData     []lo.Tuple2[*model.CDCData, WriteCallback]
	bufferDataChan chan []lo.Tuple2[*model.CDCData, WriteCallback]

	timetickLock                sync.Mutex
	collectionTimeTickPositions map[int64]map[string]*commonpb.KeyDataPair
	collectionNames             map[int64]string

	hasInsertMsg bool
	hasDeleteMsg bool
}

// NewCDCWriterTemplate options must include HandlerOption
func NewCDCWriterTemplate(options ...config.Option[*CDCWriterTemplate]) CDCWriter {
	c := &CDCWriterTemplate{
		bufferConfig: DefaultBufferConfig,
		errProtect:   FastFail(),
	}
	for _, option := range options {
		option.Apply(c)
	}
	c.funcMap = map[msgstream.MsgType]func(context.Context, *model.CDCData, WriteCallback){
		commonpb.MsgType_CreateCollection:  c.handleCreateCollection,
		commonpb.MsgType_DropCollection:    c.handleDropCollection,
		commonpb.MsgType_Insert:            c.handleInsert,
		commonpb.MsgType_Delete:            c.handleDelete,
		commonpb.MsgType_CreatePartition:   c.handleCreatePartition,
		commonpb.MsgType_DropPartition:     c.handleDropPartition,
		commonpb.MsgType_TimeTick:          c.handleTimeTick,
		commonpb.MsgType_CreateIndex:       c.handleRPCRequest,
		commonpb.MsgType_DropIndex:         c.handleRPCRequest,
		commonpb.MsgType_LoadCollection:    c.handleRPCRequest,
		commonpb.MsgType_ReleaseCollection: c.handleRPCRequest,
		commonpb.MsgType_CreateDatabase:    c.handleRPCRequest,
		commonpb.MsgType_DropDatabase:      c.handleRPCRequest,
	}
	c.initBuffer()
	c.periodFlush()
	return c
}

func (c *CDCWriterTemplate) initBuffer() {
	c.bufferDataChan = make(chan []lo.Tuple2[*model.CDCData, WriteCallback])

	// execute buffer ops
	go func() {
		for {
			select {
			case <-c.errProtect.Chan():
				log.Warn("the error protection is triggered", zap.String("protect", c.errProtect.Info()))
				return
			default:
			}

			latestPositions := make(map[int64]map[string]*commonpb.KeyDataPair)
			collectionNames := make(map[int64]string)
			positionFunc := NotifyCollectionPositionChangeFunc(func(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) {
				if position == nil {
					return
				}
				collectionNames[collectionID] = collectionName
				collectionPositions, ok := latestPositions[collectionID]
				if !ok {
					collectionPositions = make(map[string]*commonpb.KeyDataPair)
					latestPositions[collectionID] = collectionPositions
				}
				collectionPositions[pChannelName] = position
			})

			bufferData := <-c.bufferDataChan
			if len(bufferData) == 0 {
				c.timetickLock.Lock()
				var infosString []string
				for collectionID, collectionPositions := range c.collectionTimeTickPositions {
					var channels []string
					for pChannelName, position := range collectionPositions {
						c.bufferUpdatePositionFunc(collectionID, c.collectionNames[collectionID], pChannelName, position)
						channels = append(channels, pChannelName)
					}
					infosString = append(infosString, fmt.Sprintf("%s-%d-%v", c.collectionNames[collectionID], collectionID, channels))
				}

				log.Info("update position from timetick msg",
					zap.String("collection_timetick", strings.Join(infosString, ",")))
				c.resetCollectionTimeTickPositions()
				c.timetickLock.Unlock()
				continue
			}
			c.timetickLock.Lock()
			c.resetCollectionTimeTickPositions()
			c.timetickLock.Unlock()

			logCollectionID := int64(0)
			logCollectionName := ""

			if x, ok := bufferData[0].A.Msg.(interface{ GetCollectionName() string }); ok {
				logCollectionName = x.GetCollectionName()
			}

			if y, ok := bufferData[0].A.Msg.(interface{ GetCollectionID() int64 }); ok {
				logCollectionID = y.GetCollectionID()
			}

			log.Info("handle buffer data",
				zap.Int("size", len(bufferData)),
				zap.String("msg_type", bufferData[0].A.Msg.Type().String()),
				zap.Int64("collection_id", logCollectionID),
				zap.String("collection_name", logCollectionName))
			combineDataMap := make(map[string][]*CombineData)
			c.combineDataFunc(bufferData, combineDataMap, positionFunc)
			executeSuccesses := func(successes []func()) {
				for _, success := range successes {
					success()
				}
			}
			executeFails := func(fails []func(err error), err error) {
				for _, fail := range fails {
					fail(err)
				}
			}

			params := make([][]*CombineData, 10, 10)
			ctx := context.Background()
			for _, combineDatas := range combineDataMap {
				for _, combineData := range combineDatas {
					switch combineData.param.(type) {
					case *InsertParam:
						params[0] = append(params[0], combineData)
					case *CreateIndexParam:
						params[1] = append(params[1], combineData)
					case *LoadCollectionParam:
						params[2] = append(params[2], combineData)
					case *CreateDataBaseParam:
						params[3] = append(params[3], combineData)
					case *ReleaseCollectionParam:
						params[4] = append(params[4], combineData)
					case *DropIndexParam:
						params[5] = append(params[5], combineData)
					case *DeleteParam:
						params[6] = append(params[6], combineData)
					case *DropPartitionParam:
						params[7] = append(params[7], combineData)
					case *DropCollectionParam:
						params[8] = append(params[8], combineData)
					case *DropDataBaseParam:
						params[9] = append(params[9], combineData)
					default:
						log.Warn("invalid param", zap.Any("data", combineData))
						continue
					}
				}
			}

			for _, ps := range params {
				for _, combineData := range ps {
					var err error
					switch p := combineData.param.(type) {
					case *InsertParam:
						err = c.handler.Insert(ctx, p)
					case *DeleteParam:
						err = c.handler.Delete(ctx, p)
					case *DropCollectionParam:
						err = c.handler.DropCollection(ctx, p)
					case *DropPartitionParam:
						err = c.handler.DropPartition(ctx, p)
					case *CreateIndexParam:
						err = c.handler.CreateIndex(ctx, p)
					case *DropIndexParam:
						err = c.handler.DropIndex(ctx, p)
					case *LoadCollectionParam:
						err = c.handler.LoadCollection(ctx, p)
					case *ReleaseCollectionParam:
						err = c.handler.ReleaseCollection(ctx, p)
					case *CreateDataBaseParam:
						err = c.handler.CreateDatabase(ctx, p)
					case *DropDataBaseParam:
						err = c.handler.DropDatabase(ctx, p)
					default:
						continue
					}
					if err != nil {
						executeFails(combineData.fails, err)
						continue
					}
					executeSuccesses(combineData.successes)
				}
			}

			if c.bufferUpdatePositionFunc != nil {
				for collectionID, collectionPositions := range latestPositions {
					for pChannelName, position := range collectionPositions {
						c.bufferUpdatePositionFunc(collectionID, collectionNames[collectionID], pChannelName, position)
						log.Info("update position",
							zap.Int64("collection_id", collectionID),
							zap.String("collection_name", collectionNames[collectionID]),
							zap.String("channel_name", pChannelName))
					}
				}
			}
		}
	}()
}

type CombineData struct {
	param     any
	fails     []func(err error)
	successes []func()
}

func (c *CDCWriterTemplate) combineDataFunc(dataArr []lo.Tuple2[*model.CDCData, WriteCallback],
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc) {

	for _, tuple := range dataArr {
		data := tuple.A
		callback := tuple.B
		switch msg := data.Msg.(type) {
		case *msgstream.InsertMsg:
			c.handleInsertBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DeleteMsg:
			c.handleDeleteBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DropCollectionMsg:
			c.handleDropCollectionBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DropPartitionMsg:
			c.handleDropPartitionBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.CreateIndexMsg:
			c.handleCreateIndexBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DropIndexMsg:
			c.handleDropIndexBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.LoadCollectionMsg:
			c.handleLoadCollectionBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.ReleaseCollectionMsg:
			c.handleReleaseCollectionBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.CreateDatabaseMsg:
			c.handleCreateDatabaseBuffer(msg, data, callback, combineDataMap, positionFunc)
		case *msgstream.DropDatabaseMsg:
			c.handleDropDatabaseBuffer(msg, data, callback, combineDataMap, positionFunc)
		}
	}
}

func (c *CDCWriterTemplate) generateBufferKey(a string, b string) string {
	return a + ":" + b
}

func (c *CDCWriterTemplate) handleInsertBuffer(msg *msgstream.InsertMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {

	collectionName := msg.CollectionName
	partitionName := msg.PartitionName
	dataKey := c.generateBufferKey(collectionName, partitionName)
	// construct columns
	var columns []entity.Column
	for _, fieldData := range msg.FieldsData {
		if fieldData.GetFieldName() == DynamicFieldName &&
			len(fieldData.GetScalars().GetLongData().GetData()) == 0 &&
			len(fieldData.GetScalars().GetStringData().GetData()) == 0 {
			continue
		}
		if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
			columns = append(columns, column)
		} else {
			column, err := entity.FieldDataVector(fieldData)
			if err != nil {
				c.fail("fail to parse the data", err, data, callback)
				return
			}
			columns = append(columns, column)
		}
	}
	// new combine data for convenient usage below
	newCombineData := &CombineData{
		param: &InsertParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Columns:        columns,
		},
		successes: []func(){
			func() {
				c.success(msg.CollectionID, collectionName, len(msg.RowIDs), data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to insert the data", err, data, callback)
			},
		},
	}
	combineDataArr, ok := combineDataMap[dataKey]
	// check whether the combineDataMap contains the key, if not, add the data
	if !ok {
		combineDataMap[dataKey] = []*CombineData{
			newCombineData,
		}
		return
	}
	lastCombineData := combineDataArr[len(combineDataArr)-1]
	insertParam, ok := lastCombineData.param.(*InsertParam)
	// check whether the last data is insert, if not, add the data to array
	if !ok {
		combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
		return
	}
	// combine the data
	sortColumns, err := c.preCombineColumn(insertParam.Columns, columns)
	if err != nil {
		c.fail("fail to combine the data", err, data, callback)
		return
	}
	c.combineColumn(insertParam.Columns, sortColumns)
	lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
	lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
}

func (c *CDCWriterTemplate) handleDeleteBuffer(msg *msgstream.DeleteMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	collectionName := msg.CollectionName
	partitionName := msg.PartitionName
	dataKey := c.generateBufferKey(collectionName, partitionName)
	// get the id column
	column, err := entity.IDColumns(msg.PrimaryKeys, 0, -1)
	if err != nil {
		c.fail("fail to get the id columns", err, data, callback)
		return
	}
	newCombineData := &CombineData{
		param: &DeleteParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Column:         column,
		},
		successes: []func(){
			func() {
				c.success(msg.CollectionID, collectionName, int(msg.NumRows), data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to delete the column", err, data, callback)
			},
		},
	}
	combineDataArr, ok := combineDataMap[dataKey]
	// check whether the combineDataMap contains the key, if not, add the data
	if !ok {
		combineDataMap[dataKey] = []*CombineData{
			newCombineData,
		}
		return
	}
	lastCombineData := combineDataArr[len(combineDataArr)-1]
	deleteParam, ok := lastCombineData.param.(*DeleteParam)
	// check whether the last data is insert, if not, add the data to array
	if !ok {
		combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
		return
	}
	// combine the data
	var values []interface{}
	switch columnValue := column.(type) {
	case *entity.ColumnInt64:
		for _, id := range columnValue.Data() {
			values = append(values, id)
		}
	case *entity.ColumnVarChar:
		for _, varchar := range columnValue.Data() {
			values = append(values, varchar)
		}
	default:
		c.fail("fail to combine the delete data", err, data, callback)
	}
	for _, value := range values {
		err = deleteParam.Column.AppendValue(value)
		if err != nil {
			c.fail("fail to combine the delete data", err, data, callback)
			return
		}
	}
	lastCombineData.successes = append(lastCombineData.successes, newCombineData.successes...)
	lastCombineData.fails = append(lastCombineData.fails, newCombineData.fails...)
}

func (c *CDCWriterTemplate) handleDropCollectionBuffer(msg *msgstream.DropCollectionMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	collectionName := msg.CollectionName
	dataKey := c.generateBufferKey(collectionName, "")
	newCombineData := &CombineData{
		param: &DropCollectionParam{
			CollectionName: collectionName,
		},
		successes: []func(){
			func() {
				channelInfos := make(map[string]CallbackChannelInfo)
				collectChannelInfo := func(dropCollectionMsg *msgstream.DropCollectionMsg) {
					position := dropCollectionMsg.Position()
					kd := &commonpb.KeyDataPair{
						Key:  position.ChannelName,
						Data: position.MsgID,
					}
					channelInfos[position.ChannelName] = CallbackChannelInfo{
						Position: kd,
						Ts:       dropCollectionMsg.EndTs(),
					}
				}
				collectChannelInfo(msg)
				if msgsValue := data.Extra[model.DropCollectionMsgsKey]; msgsValue != nil {
					msgs := msgsValue.([]*msgstream.DropCollectionMsg)
					for _, tsMsg := range msgs {
						collectChannelInfo(tsMsg)
					}
				}

				callback.OnSuccess(msg.CollectionID, channelInfos)
				if positionFunc != nil {
					for _, info := range channelInfos {
						positionFunc(msg.CollectionID, msg.CollectionName, info.Position.Key, info.Position)
					}
				}
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to drop collection", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleDropPartitionBuffer(msg *msgstream.DropPartitionMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	collectionName := msg.CollectionName
	partitionName := msg.PartitionName
	dataKey := c.generateBufferKey(collectionName, partitionName)
	newCombineData := &CombineData{
		param: &DropPartitionParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		successes: []func(){
			func() {
				channelInfos := make(map[string]CallbackChannelInfo)
				collectChannelInfo := func(dropPartitionMsg *msgstream.DropPartitionMsg) {
					position := dropPartitionMsg.Position()
					kd := &commonpb.KeyDataPair{
						Key:  position.ChannelName,
						Data: position.MsgID,
					}
					channelInfos[position.ChannelName] = CallbackChannelInfo{
						Position: kd,
						Ts:       dropPartitionMsg.EndTs(),
					}
				}
				collectChannelInfo(msg)
				if msgsValue := data.Extra[model.DropPartitionMsgsKey]; msgsValue != nil {
					msgs := msgsValue.([]*msgstream.DropPartitionMsg)
					for _, tsMsg := range msgs {
						collectChannelInfo(tsMsg)
					}
				}

				callback.OnSuccess(msg.CollectionID, channelInfos)
				if positionFunc != nil {
					for _, info := range channelInfos {
						positionFunc(msg.CollectionID, msg.CollectionName, info.Position.Key, info.Position)
					}
				}
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to drop collection", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleCreateIndexBuffer(msg *msgstream.CreateIndexMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("create_index_%s_%s_%d", msg.CollectionName, msg.IndexName, rand.Int())
	newCombineData := &CombineData{
		param: &CreateIndexParam{
			CreateIndexRequest: msg.CreateIndexRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to create index", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleDropIndexBuffer(msg *msgstream.DropIndexMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("drop_index_%s_%s_%d", msg.CollectionName, msg.IndexName, rand.Int())
	newCombineData := &CombineData{
		param: &DropIndexParam{
			DropIndexRequest: msg.DropIndexRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to drop index", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleLoadCollectionBuffer(msg *msgstream.LoadCollectionMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("load_collection_%s_%d", msg.CollectionName, rand.Int())
	newCombineData := &CombineData{
		param: &LoadCollectionParam{
			LoadCollectionRequest: msg.LoadCollectionRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to load collection", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleReleaseCollectionBuffer(msg *msgstream.ReleaseCollectionMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("release_collection_%s_%d", msg.CollectionName, rand.Int())
	newCombineData := &CombineData{
		param: &ReleaseCollectionParam{
			ReleaseCollectionRequest: msg.ReleaseCollectionRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to release collection", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleCreateDatabaseBuffer(msg *msgstream.CreateDatabaseMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("create_database_%s_%d", msg.DbName, rand.Int())
	newCombineData := &CombineData{
		param: &CreateDataBaseParam{
			CreateDatabaseRequest: msg.CreateDatabaseRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to create database", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) handleDropDatabaseBuffer(msg *msgstream.DropDatabaseMsg,
	data *model.CDCData, callback WriteCallback,
	combineDataMap map[string][]*CombineData,
	positionFunc NotifyCollectionPositionChangeFunc,
) {
	dataKey := fmt.Sprintf("drop_database_%s_%d", msg.DbName, rand.Int())
	newCombineData := &CombineData{
		param: &DropDataBaseParam{
			DropDatabaseRequest: msg.DropDatabaseRequest,
		},
		successes: []func(){
			func() {
				c.rpcRequestSuccess(msg, data, callback, positionFunc)
			},
		},
		fails: []func(err error){
			func(err error) {
				c.fail("fail to drop database", err, data, callback)
			},
		},
	}
	combineDataMap[dataKey] = append(combineDataMap[dataKey], newCombineData)
}

func (c *CDCWriterTemplate) rpcRequestSuccess(msg msgstream.TsMsg, data *model.CDCData, callback WriteCallback, positionFunc NotifyCollectionPositionChangeFunc) {
	channelInfos := make(map[string]CallbackChannelInfo)
	position := msg.Position()
	info := CallbackChannelInfo{
		Position: &commonpb.KeyDataPair{
			Key:  position.ChannelName,
			Data: position.MsgID,
		},
		Ts: msg.EndTs(),
	}
	channelInfos[position.ChannelName] = info
	collectionID := util.RPCRequestCollectionID
	collectionName := util.RPCRequestCollectionName
	if value, ok := data.Extra[model.CollectionIDKey]; ok {
		collectionID = value.(int64)
	}
	if value, ok := data.Extra[model.CollectionNameKey]; ok {
		collectionName = value.(string)
	}
	callback.OnSuccess(collectionID, channelInfos)
	if positionFunc != nil {
		positionFunc(collectionID, collectionName, info.Position.Key, info.Position)
	}
}

func (c *CDCWriterTemplate) periodFlush() {
	go func() {
		if c.bufferConfig.Period <= 0 {
			return
		}
		log.Info("start period flush",
			zap.Duration("period", c.bufferConfig.Period),
			zap.Int64("size_kb", c.bufferConfig.Size/1024))
		ticker := time.NewTicker(c.bufferConfig.Period)
		for {
			<-ticker.C
			c.Flush(context.Background())
		}
	}()
}

func (c *CDCWriterTemplate) Write(ctx context.Context, data *model.CDCData, callback WriteCallback) error {
	select {
	case <-c.errProtect.Chan():
		log.Warn("the error protection is triggered", zap.String("protect", c.errProtect.Info()))
		return errors.New("the error protection is triggered")
	default:
	}

	handleFunc, ok := c.funcMap[data.Msg.Type()]
	if !ok {
		// don't execute the fail callback, because the future messages will be ignored and don't trigger the error protection
		log.Warn("not support message type", zap.Any("data", data))
		return fmt.Errorf("not support message type, type: %s", data.Msg.Type().String())
	}
	handleFunc(ctx, data, callback)
	return nil
}

func (c *CDCWriterTemplate) Flush(context context.Context) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) handleCreateCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.CreateCollectionMsg)
	schema := &schemapb.CollectionSchema{}
	err := json.Unmarshal(msg.Schema, schema)
	if err != nil {
		c.fail("fail to unmarshal the collection schema", err, data, callback)
		return
	}
	var shardNum int32
	if value, ok := data.Extra[model.ShardNumKey]; ok {
		shardNum = value.(int32)
	}
	level := commonpb.ConsistencyLevel_Strong
	if value, ok := data.Extra[model.ConsistencyLevelKey]; ok {
		level = value.(commonpb.ConsistencyLevel)
	}
	var properties []*commonpb.KeyValuePair
	if value, ok := data.Extra[model.CollectionPropertiesKey]; ok {
		properties = value.([]*commonpb.KeyValuePair)
	}

	entitySchema := &entity.Schema{}
	entitySchema = entitySchema.ReadProto(schema)
	err = c.handler.CreateCollection(ctx, &CreateCollectionParam{
		ID:               msg.CollectionID,
		Schema:           entitySchema,
		ShardsNum:        shardNum,
		ConsistencyLevel: level,
		Properties:       properties,
	})
	if err != nil {
		c.fail("fail to create the collection", err, data, callback)
		return
	}
	callback.OnSuccess(msg.CollectionID, nil)
}

func (c *CDCWriterTemplate) handleDropCollection(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) handleInsert(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.InsertMsg)
	totalSize := int64(msg.Size())
	log.Info("insert msg", zap.Int64("size", totalSize))
	//if totalSize < 0 {
	//	c.fail("fail to get the data size", errors.New("invalid column type"), data, callback)
	//	return
	//}

	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	if c.hasDeleteMsg {
		c.clearBufferFunc()
	}
	c.hasInsertMsg = true
	c.currentBufferSize += totalSize
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CDCWriterTemplate) handleDelete(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.DeleteMsg)
	totalSize := int64(msg.Size())

	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	if c.hasInsertMsg {
		c.clearBufferFunc()
	}
	c.hasDeleteMsg = true
	c.currentBufferSize += totalSize
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.checkBufferSize()
}

func (c *CDCWriterTemplate) handleCreatePartition(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	msg := data.Msg.(*msgstream.CreatePartitionMsg)
	err := c.handler.CreatePartition(ctx, &CreatePartitionParam{
		CollectionName: msg.CollectionName,
		PartitionName:  msg.PartitionName,
	})
	if err != nil {
		c.fail("fail to create the partition", err, data, callback)
		return
	}
	callback.OnSuccess(msg.CollectionID, nil)
}

func (c *CDCWriterTemplate) handleDropPartition(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) handleTimeTick(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	c.timetickLock.Lock()
	defer c.timetickLock.Unlock()

	if c.collectionTimeTickPositions == nil || c.collectionNames == nil {
		c.resetCollectionTimeTickPositions()
	}
	tsMsg := data.Msg.(*msgstream.TimeTickMsg)
	value, ok := data.Extra[model.CollectionIDKey]
	if !ok {
		log.Warn("leak the collection id")
		return
	}
	nameValue, ok := data.Extra[model.CollectionNameKey]
	if !ok {
		log.Warn("leak the collection name")
		return
	}
	collectionID := value.(int64)
	channelPositions := c.collectionTimeTickPositions[collectionID]
	if channelPositions == nil {
		channelPositions = make(map[string]*commonpb.KeyDataPair)
		c.collectionTimeTickPositions[collectionID] = channelPositions
	}
	position := tsMsg.Position()
	channelPositions[position.GetChannelName()] = &commonpb.KeyDataPair{
		Key:  position.GetChannelName(),
		Data: position.GetMsgID(),
	}
	c.collectionNames[collectionID] = nameValue.(string)
}

func (c *CDCWriterTemplate) resetCollectionTimeTickPositions() {
	c.collectionTimeTickPositions = make(map[int64]map[string]*commonpb.KeyDataPair)
	c.collectionNames = make(map[int64]string)
}

func (c *CDCWriterTemplate) handleRPCRequest(ctx context.Context, data *model.CDCData, callback WriteCallback) {
	c.bufferLock.Lock()
	defer c.bufferLock.Unlock()
	c.bufferData = append(c.bufferData, lo.T2(data, callback))
	c.clearBufferFunc()
}

func (c *CDCWriterTemplate) collectionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetCollectionName() string })
	if ok {
		return f.GetCollectionName()
	}
	return ""
}

func (c *CDCWriterTemplate) partitionName(data *model.CDCData) string {
	f, ok := data.Msg.(interface{ GetPartitionName() string })
	if ok {
		return f.GetPartitionName()
	}
	return ""
}

func (c *CDCWriterTemplate) fail(msg string, err error, data *model.CDCData,
	callback WriteCallback, field ...zap.Field) {

	log.Warn(msg, append(field,
		zap.String("collection_name", c.collectionName(data)),
		zap.String("partition_name", c.partitionName(data)),
		zap.Error(err))...)
	callback.OnFail(data, errors.WithMessage(err, msg))
	c.errProtect.Inc()
}

func (c *CDCWriterTemplate) success(collectionID int64, collectionName string, rowCount int,
	data *model.CDCData, callback WriteCallback, positionFunc NotifyCollectionPositionChangeFunc) {
	position := data.Msg.Position()
	kd := &commonpb.KeyDataPair{
		Key:  position.ChannelName,
		Data: position.MsgID,
	}
	callback.OnSuccess(collectionID, map[string]CallbackChannelInfo{
		position.ChannelName: {
			Position:    kd,
			MsgType:     data.Msg.Type(),
			MsgRowCount: rowCount,
			Ts:          data.Msg.EndTs(),
		},
	})
	if positionFunc != nil {
		positionFunc(collectionID, collectionName, position.ChannelName, kd)
	}
}

func (c *CDCWriterTemplate) checkBufferSize() {
	if c.currentBufferSize >= c.bufferConfig.Size {
		c.clearBufferFunc()
	}
}

func (c *CDCWriterTemplate) clearBufferFunc() {
	// no copy, is a shallow copy
	c.bufferDataChan <- c.bufferData[:]
	c.bufferData = []lo.Tuple2[*model.CDCData, WriteCallback]{}
	c.currentBufferSize = 0
	c.hasInsertMsg = false
	c.hasDeleteMsg = false
}

func (c *CDCWriterTemplate) isSupportType(fieldType entity.FieldType) bool {
	return fieldType == entity.FieldTypeBool ||
		fieldType == entity.FieldTypeInt8 ||
		fieldType == entity.FieldTypeInt16 ||
		fieldType == entity.FieldTypeInt32 ||
		fieldType == entity.FieldTypeInt64 ||
		fieldType == entity.FieldTypeFloat ||
		fieldType == entity.FieldTypeDouble ||
		fieldType == entity.FieldTypeString ||
		fieldType == entity.FieldTypeVarChar ||
		fieldType == entity.FieldTypeBinaryVector ||
		fieldType == entity.FieldTypeFloatVector ||
		fieldType == entity.FieldTypeJSON
}

func (c *CDCWriterTemplate) preCombineColumn(a []entity.Column, b []entity.Column) ([]entity.Column, error) {
	sortColumn := make([]entity.Column, 0, len(b))
	for i := range a {
		isExist := false
		for j := range b {
			if a[i].Name() == b[j].Name() {
				if a[i].Type() != b[j].Type() || !c.isSupportType(b[j].Type()) {
					log.Warn("fail to combine the column",
						zap.String("column_name", a[i].Name()),
						zap.Any("a", a[i].Type()), zap.Any("b", b[j].Type()))
					return sortColumn, errors.New("fail to combine the column")
				}
				sortColumn = append(sortColumn, b[j])
				isExist = true
				break
			}
		}
		if !isExist {
			log.Warn("fail to combine the column, not found column", zap.String("column_name", a[i].Name()))
			return sortColumn, errors.New("fail to combine the column, not found column")
		}
	}
	return sortColumn, nil
}

// combineColumn the b will be added to a. before execute the method, MUST execute the preCombineColumn
func (c *CDCWriterTemplate) combineColumn(a []entity.Column, b []entity.Column) {
	for i := range a {
		var values []interface{}
		switch columnValue := b[i].(type) {
		case *entity.ColumnBool:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt8:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt16:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt32:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnInt64:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnFloat:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnDouble:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnString:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnVarChar:
			for _, varchar := range columnValue.Data() {
				values = append(values, varchar)
			}
		case *entity.ColumnBinaryVector:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnFloatVector:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		case *entity.ColumnJSONBytes:
			for _, id := range columnValue.Data() {
				values = append(values, id)
			}
		default:
			log.Panic("not support column type", zap.Any("value", columnValue))
		}
		for _, value := range values {
			_ = a[i].AppendValue(value)
		}
	}
}
