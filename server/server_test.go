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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/core/writer"
	cdcerror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

type MockResponseWriter struct {
	t          *testing.T
	exceptCode int
	resp       []byte
}

func (m *MockResponseWriter) Header() http.Header {
	return map[string][]string{}
}

func (m *MockResponseWriter) Write(bytes []byte) (int, error) {
	m.resp = append(m.resp, bytes...)
	return len(bytes), nil
}

func (m *MockResponseWriter) WriteHeader(statusCode int) {
	assert.Equal(m.t, m.exceptCode, statusCode)
}

type MockReaderCloser struct {
	err error
}

func (m *MockReaderCloser) Read(p []byte) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(p), nil
}

func (m *MockReaderCloser) Close() error {
	return nil
}

type MockBaseCDC struct {
	BaseCDC
	resp *request.CreateResponse
	err  error
}

func (m *MockBaseCDC) Create(request *request.CreateRequest) (*request.CreateResponse, error) {
	return m.resp, m.err
}

func TestCDCHandler(t *testing.T) {
	server := &CDCServer{}
	handler := server.getCDCHandler()

	t.Run("get method", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusMethodNotAllowed,
		}
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodGet})
		assert.Contains(t, string(responseWriter.resp), "only support the POST method")
	})

	t.Run("read error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: &MockReaderCloser{err: errors.New("FOO")}})
		assert.Contains(t, string(responseWriter.resp), "fail to read the request body")
	})

	t.Run("unmarshal error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader([]byte("fooooo")))})
		assert.Contains(t, string(responseWriter.resp), "fail to unmarshal the request")
	})

	t.Run("request type error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusBadRequest,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: "foo",
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader(requestBytes))})
		assert.Contains(t, string(responseWriter.resp), "invalid 'request_type' param")
	})

	t.Run("request data format error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{
				"buffer_config": 10001,
			},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader(requestBytes))})
		assert.Contains(t, string(responseWriter.resp), "fail to decode the create request")
	})

	t.Run("request data handle server error", func(t *testing.T) {
		server.api = &MockBaseCDC{
			err: errors.New("foo"),
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader(requestBytes))})
		assert.Contains(t, string(responseWriter.resp), "fail to handle the create request")
	})

	t.Run("request data handle client error", func(t *testing.T) {
		server.api = &MockBaseCDC{
			err: cdcerror.NewClientError("foo"),
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusBadRequest,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader(requestBytes))})
		assert.Contains(t, string(responseWriter.resp), "fail to handle the create request")
	})

	t.Run("request success", func(t *testing.T) {
		taskID := "123456789"
		server.api = &MockBaseCDC{
			resp: &request.CreateResponse{
				TaskID: taskID,
			},
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusOK,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodPost,
			Body: io.NopCloser(bytes.NewReader(requestBytes))})
		assert.Contains(t, string(responseWriter.resp), taskID)
	})
}

func SkipTestDynamicSchema(t *testing.T) {
	//base64Data := "ChkIkAMQwrKQx+nHm5gGGIaAsO/S0ZuYBiAFEitieS1kZXYtcm9vdGNvb3JkLWRtbF8yXzQ0NTk3NDcxMTUwMDgxOTYzOXYwIgxoZWxsb19taWx2dXMqCF9kZWZhdWx0OLeZwY6y9JqYBkC4mcGOsvSamAZI+sycx+nHm5gGUlqGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAaGgLDv0tGbmAZaWriykMfpx5uYBrmykMfpx5uYBrqykMfpx5uYBruykMfpx5uYBryykMfpx5uYBr2ykMfpx5uYBr6ykMfpx5uYBr+ykMfpx5uYBsCykMfpx5uYBsGykMfpx5uYBmrbAghlEgplbWJlZGRpbmdzKGUiyAIICBLDAgrAArlLIz+D5eA+YEkHPhT17z43ij4//q8CPVpCoj7kPxo/0JZmP9Ud5j71NT0+n83bPvSRzj7Cm8o+mEYyPyxqeT7fqdk+VIsSP9q52j5/z2E/j0NYP24TMT+j74o+Te15P40oHT652TU/wWgdPgUeqz4b+Hc/ZHElP40nYj/65Cg/f5oSPyaabz/OfEI+/v7APlBQoT5FgI0+JL8bP5W2iz1lCgk88NE3P3u9VT86Vyk/Bs17P3VZIj4laZ4+PTBxPnTBRz6FuU0/Ik4xPjHTlz3/mlo/LzdPP4CFGz0n6Ys+i+fwPRifMT9vxEY++tJPPy+MZD81Ng4/3E9jP9FS8T5ZZp8+mNc7P1bKuz5wCWA+VjuBPpmCUj85KVc+rSssPncaUD9kfT8/zJ6MPhGamT1ByRM/aV0qPyOuXj8+UcM+amIICxIGcmFuZG9tKGYaVCpSClAraWzlxz3sPyUGBHo6y+I/+UAgdOBT6j9Q/g1jFtu3P4Trg80wyOs/KIYvnY1U5D9yaYmLM8njP0HRKjS5duU/WFIFr7x05z8SdeWLlifhP2o5CBcSBSRtZXRhKGcwARoqSigKAnt9CgJ7fQoCe30KAnt9CgJ7fQoCe30KAnt9CgJ7fQoCe30KAnt9amgIBRICcGsoZBpeGlwKWriykMfpx5uYBrmykMfpx5uYBrqykMfpx5uYBruykMfpx5uYBryykMfpx5uYBr2ykMfpx5uYBr6ykMfpx5uYBr+ykMfpx5uYBsCykMfpx5uYBsGykMfpx5uYBnAKeAE="
	base64Data := "ChkIkAMQybKQx+nHm5gGGIeA0JGE2ZuYBiAFEitieS1kZXYtcm9vdGNvb3JkLWRtbF8yXzQ0NTk3NDcxMTUwMDgxOTYzOXYwIgxoZWxsb19taWx2dXMqCF9kZWZhdWx0OLeZwY6y9JqYBkC4mcGOsvSamAZIt42px+nHm5gGUjaHgNCRhNmbmAaHgNCRhNmbmAaHgNCRhNmbmAaHgNCRhNmbmAaHgNCRhNmbmAaHgNCRhNmbmAZaNsOykMfpx5uYBsSykMfpx5uYBsWykMfpx5uYBsaykMfpx5uYBseykMfpx5uYBsiykMfpx5uYBmrbAQhlEgplbWJlZGRpbmdzKGUiyAEICBLDAQrAAblLIz+D5eA+YEkHPhT17z43ij4//q8CPVpCoj7kPxo/0JZmP9Ud5j71NT0+n83bPvSRzj7Cm8o+mEYyPyxqeT7fqdk+VIsSP9q52j5/z2E/j0NYP24TMT+j74o+Te15P40oHT652TU/wWgdPgUeqz4b+Hc/ZHElP40nYj/65Cg/f5oSPyaabz/OfEI+/v7APlBQoT5FgI0+JL8bP5W2iz1lCgk88NE3P3u9VT86Vyk/Bs17P3VZIj4laZ4+PTBxPmpCCAsSBnJhbmRvbShmGjQqMgowAAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/akcIFxIFJG1ldGEoZzABGjhKNgoHeyJhIjoxfQoHeyJiIjoxfQoHeyJjIjoxfQoHeyJkIjoxfQoHeyJlIjoxfQoHeyJmIjoxfWpECAUSAnBrKGQaOho4CjbDspDH6cebmAbEspDH6cebmAbFspDH6cebmAbGspDH6cebmAbHspDH6cebmAbIspDH6cebmAZwBngB"
	byteData, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		panic(err)
	}
	data := &msgstream.InsertMsg{}
	tsMsg, err := data.Unmarshal(byteData)
	if err != nil {
		panic(err)
	}

	factory := writer.NewDefaultMilvusClientFactory()
	timeoutContext, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	//address := "in01-55013270bc2743c.aws-us-west-2.vectordb-uat3.zillizcloud.com:19533"
	//username := "root"
	//password := "q3.5,8A:-1RZ+7{B7z<Cpe!U|vT[k%>U"
	//milvusClient, err := factory.NewGrpcClientWithTLSAuth(timeoutContext, address, username, password)

	address := "localhost:19530"
	username := "root"
	password := "Milvus"
	milvusClient, err := factory.NewGrpcClientWithAuth(timeoutContext, address, username, password)
	if err != nil {
		panic(err)
	}

	msg := tsMsg.(*msgstream.InsertMsg)
	var columns []entity.Column
	for _, fieldData := range msg.FieldsData {
		if fieldData.GetType() == schemapb.DataType_JSON {
			data, _ := fieldData.GetScalars().GetData().(*schemapb.ScalarField_JsonData)
			for i, x := range data.JsonData.GetData() {
				log.Info("json data", zap.String("data", string(x)), zap.Int("index", i))
			}
		}
		if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
			columns = append(columns, column)
		} else {
			column, err := entity.FieldDataVector(fieldData)
			if err != nil {
				panic(err)
			}
			columns = append(columns, column)
		}
	}
	result, err := milvusClient.Insert(timeoutContext, msg.CollectionName, msg.PartitionName, columns...)
	if err != nil {
		panic(err)
	}
	log.Info("result", zap.Any("result", result))
}

func TestGetMsgPosition(t *testing.T) {
	data := []byte("CAEQ9pUBGAAgAA==")
	channelName := "by-dev-rootcoord-dml_1"
	position := util.Base64MsgPosition(&msgstream.MsgPosition{
		ChannelName: channelName,
		MsgID:       data,
	})
	kp, _ := decodePosition(channelName, position)
	log.Info("position", zap.Any("position", position), zap.Any("kp", kp))
}

func TestMapOutput(t *testing.T) {
	m := map[string]int{
		"a": 1,
		"b": 2,
	}
	fmt.Println("current:", m)
}

func TestDecodePosition(t *testing.T) {
	position := "CjlpbjAxLWRhMTIxODgxZTNiZDUyYS1yb290Y29vcmQtZG1sXzFfNDQ2MDE3ODUyOTk3NjIxMzMxdjASDQjSgekDEKmvARgAIAAaWmluMDEtZGExMjE4ODFlM2JkNTJhLWRhdGFOb2RlLTIxLWluMDEtZGExMjE4ODFlM2JkNTJhLXJvb3Rjb29yZC1kbWxfMV80NDYwMTc4NTI5OTc2MjEzMzF2MCCBgLDCuKilmAY="
	positionBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		panic(err)
	}
	msgPosition := &msgpb.MsgPosition{}
	err = proto.Unmarshal(positionBytes, msgPosition)
	if err != nil {
		panic(err)
	}
	log.Info("position", zap.Any("position", msgPosition))

	msgTime := tsoutil.PhysicalTime(msgPosition.Timestamp)
	log.Info("time", zap.Time("time", msgTime))
}

func TestTS(t *testing.T) {
	msgTime := tsoutil.PhysicalTime(446020481283260418)
	log.Info("time", zap.Time("time", msgTime))
}
