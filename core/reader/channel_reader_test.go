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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func NewChannelReaderWithFactory(channelName, seekPosition string,
	mqConfig config.MQConfig,
	dataHandler func(context.Context, *msgstream.MsgPack) bool,
	creator FactoryCreator,
) (api.Reader, error) {
	channelReader := &ChannelReader{
		channelName:          channelName,
		dataHandler:          dataHandler,
		subscriptionPosition: common.SubscriptionPositionUnknown,
	}
	if seekPosition == "" {
		channelReader.subscriptionPosition = common.SubscriptionPositionLatest
	}
	channelReader.isQuit.Store(false)
	err := channelReader.decodeSeekPosition(seekPosition)
	if err != nil {
		log.Warn("fail to seek the position", zap.Error(err))
		return nil, err
	}
	closeFunc, err := channelReader.initMsgStream(mqConfig, creator)
	if err != nil {
		log.Warn("fail to init the msg stream", zap.Error(err))
		return nil, err
	}

	channelReader.startOnceFunc = util.OnceFuncWithContext(func(ctx context.Context) {
		go channelReader.readPack(ctx)
	})
	channelReader.quitOnceFunc = sync.OnceFunc(func() {
		channelReader.isQuit.Store(true)
		closeFunc()
	})

	return channelReader, nil
}

func TestNewChannelReader(t *testing.T) {
	t.Run("channel mq", func(t *testing.T) {
		creator := mocks.NewFactoryCreator(t)
		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)

		creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
		{
			// wrong format seek position
			factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
				return nil, errors.New("error")
			}
			_, err := NewChannelReaderWithFactory("test", "test", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// msg stream error
			factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
				return nil, errors.New("error")
			}
			_, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}

		{
			// as consumer error
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error")).Once()
			_, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// seek error
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error")).Once()
			msgPosition := &msgstream.MsgPosition{
				ChannelName: "test",
				MsgID:       []byte("100"),
			}
			_, err := NewChannelReaderWithFactory("test", util.Base64MsgPosition(msgPosition), config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// success
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			packChan := make(chan *msgstream.ConsumeMsgPack, 1)
			stream.EXPECT().Chan().Return(packChan)
			msgPosition := &msgstream.MsgPosition{
				ChannelName: "test",
				MsgID:       []byte("100"),
			}
			_, err := NewChannelReaderWithFactory("test", util.Base64MsgPosition(msgPosition), config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
				Kafka: config.KafkaConfig{},
			}, nil, creator)
			assert.NoError(t, err)
			time.Sleep(time.Second)
		}
	})

	t.Run("mq config", func(t *testing.T) {
		creator := mocks.NewFactoryCreator(t)
		{
			_, err := NewChannelReaderWithFactory("test", "", config.MQConfig{}, nil, creator)
			assert.Error(t, err)
		}

		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		packChan := make(chan *msgstream.ConsumeMsgPack, 1)
		stream.EXPECT().Chan().Return(packChan)
		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}
		{
			creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory).Once()
			_, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.NoError(t, err)
			time.Sleep(500 * time.Millisecond)
		}
		{
			creator.EXPECT().NewKmsFactory(mock.Anything).Return(factory).Once()
			_, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
				Kafka: config.KafkaConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.NoError(t, err)
			time.Sleep(500 * time.Millisecond)
		}
	})
}

func TestChannelReader(t *testing.T) {
	ctx := context.Background()
	creator := mocks.NewFactoryCreator(t)
	factory := msgstream.NewMockMqFactory()
	stream := msgstream.NewMockMsgStream(t)
	f := &msgstream.ProtoUDFactory{}
	dispatcher := f.NewUnmarshalDispatcher()
	stream.EXPECT().GetUnmarshalDispatcher().Return(dispatcher).Maybe()

	creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
	factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
		return stream, nil
	}
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	t.Run("quit", func(t *testing.T) {
		dataChan := make(chan *msgstream.ConsumeMsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		stream.EXPECT().Close().Return().Once()
		reader, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		reader.QuitRead(ctx)
		reader.StartRead(ctx)
	})

	t.Run("close", func(t *testing.T) {
		dataChan := make(chan *msgstream.ConsumeMsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		reader.StartRead(ctx)
		time.Sleep(100 * time.Millisecond)
		close(dataChan)
	})

	getConsumeMsgpack := func(tt uint64, diff bool) *msgstream.ConsumeMsgPack {
		endTt := tt
		if diff {
			endTt++
		}
		ttMsg := &msgstream.TimeTickMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: tt,
				EndTimestamp:   endTt,
			},
			TimeTickMsg: &msgpb.TimeTickMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_TimeTick,
					Timestamp: tt,
				},
			},
		}
		pack := &msgstream.MsgPack{
			BeginTs: tt,
			EndTs:   endTt,
			Msgs:    []msgstream.TsMsg{ttMsg},
		}
		return msgstream.BuildConsumeMsgPack(pack)
	}

	t.Run("empty handler", func(t *testing.T) {
		dataChan := make(chan *msgstream.ConsumeMsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		dataChan <- getConsumeMsgpack(1, false)
		reader.StartRead(ctx)
		time.Sleep(time.Second)
	})

	t.Run("success", func(t *testing.T) {
		dataChan := make(chan *msgstream.ConsumeMsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReaderWithFactory("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, func(ctx context.Context, pack *msgstream.MsgPack) bool {
			return pack.BeginTs == pack.EndTs
		}, creator)
		assert.NoError(t, err)
		reader.StartRead(ctx)
		dataChan <- getConsumeMsgpack(1, false)
		dataChan <- getConsumeMsgpack(2, true)
		time.Sleep(time.Second)
	})
}
