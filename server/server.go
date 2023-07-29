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
	"encoding/json"
	"errors"
	"fmt"
	coreconfig "github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/reader"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	modelrequest "github.com/zilliztech/milvus-cdc/server/model/request"
	"go.uber.org/zap"
)

type CDCServer struct {
	api          CDCApi
	serverConfig *CDCServerConfig
}

func (c *CDCServer) Run(config *CDCServerConfig) {
	registerMetric()

	c.serverConfig = config
	c.api = GetCDCApi(c.serverConfig)
	c.api.ReloadTask()
	cdcHandler := c.getCDCHandler()
	{
		channelReader, err := reader.NewChannelReader(
			coreconfig.MilvusMQConfig{Pulsar: c.serverConfig.SourceConfig.Pulsar, Kafka: c.serverConfig.SourceConfig.Kafka},
			"by-dev-rpc-request",
			0,
			"",
			100,
		)
		if err != nil {
			log.Warn("fail to create channel reader", zap.Error(err))
		} else {
			dataChan := channelReader.StartRead(context.Background())
			go func() {
				for {
					select {
					case data := <-dataChan:
						if data == nil {
							continue
						}
						log.Info("receive data from channel", zap.Any("data", data))
					}
				}
			}()
		}
	}
	http.Handle("/cdc", cdcHandler)
	log.Info("start server...")
	err := http.ListenAndServe(c.serverConfig.Address, nil)
	log.Panic("cdc server down", zap.Error(err))
}

func (c *CDCServer) getCDCHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startTime := time.Now()
		if request.Method != http.MethodPost {
			c.handleError(writer, "only support the POST method", http.StatusMethodNotAllowed,
				zap.String("method", request.Method))
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, invalidMethodStatusLabel).Inc()
			return
		}
		bodyBytes, err := ioutil.ReadAll(request.Body)
		if err != nil {
			c.handleError(writer, "fail to read the request body, error: "+err.Error(), http.StatusInternalServerError)
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, readErrorStatusLabel).Inc()
			return
		}
		cdcRequest := &modelrequest.CDCRequest{}
		err = json.Unmarshal(bodyBytes, cdcRequest)
		if err != nil {
			c.handleError(writer, "fail to unmarshal the request, error: "+err.Error(), http.StatusInternalServerError)
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, unmarshalErrorStatusLabel).Inc()
			return
		}
		taskRequestCountVec.WithLabelValues(cdcRequest.RequestType, totalStatusLabel).Inc()

		response := c.handleRequest(cdcRequest, writer)

		if response != nil {
			_ = json.NewEncoder(writer).Encode(response)
			taskRequestCountVec.WithLabelValues(cdcRequest.RequestType, successStatusLabel).Inc()
			taskRequestLatencyVec.WithLabelValues(cdcRequest.RequestType).Observe(float64(time.Now().Sub(startTime).Milliseconds()))
		}
	})
}

func (c *CDCServer) handleError(w http.ResponseWriter, error string, code int, fields ...zap.Field) {
	log.Warn(error, fields...)
	http.Error(w, error, code)
}

func (c *CDCServer) handleRequest(cdcRequest *modelrequest.CDCRequest, writer http.ResponseWriter) any {
	requestType := cdcRequest.RequestType
	handler, ok := requestHandlers[requestType]
	if !ok {
		c.handleError(writer, fmt.Sprintf("invalid 'request_type' param, can be set %v", lo.Keys(requestHandlers)), http.StatusBadRequest,
			zap.String("type", requestType))
		return nil
	}
	requestModel := handler.generateModel()
	if err := mapstructure.Decode(cdcRequest.RequestData, requestModel); err != nil {
		c.handleError(writer, fmt.Sprintf("fail to decode the %s request, error: %s", requestType, err.Error()), http.StatusInternalServerError)
		return nil
	}
	response, err := handler.handle(c.api, requestModel)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ClientErr) {
			code = http.StatusBadRequest
		}
		c.handleError(writer, fmt.Sprintf("fail to handle the %s request, error: %s", requestType, err.Error()), code, zap.Error(err))
		return nil
	}

	return response
}
