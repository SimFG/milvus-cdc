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

package util

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/stretchr/testify/assert"
)

func TestStringAndByte(t *testing.T) {
	str := "hello"
	assert.Equal(t, []byte(str), ToBytes(str))
	assert.Equal(t, str, ToString(ToBytes(str)))
}

func TestToPhysicalChannel(t *testing.T) {
	assert.Equal(t, "abc", ToPhysicalChannel("abc_"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_123"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_defgsg"))
	assert.Equal(t, "abc__", ToPhysicalChannel("abc___defgsg"))
	assert.Equal(t, "abcdef", ToPhysicalChannel("abcdef"))
}

func TestBase64Encode(t *testing.T) {
	str := "foo"
	encodeStr := Base64Encode(str)
	assert.NotEmpty(t, encodeStr)
	decodeByte, err := base64.StdEncoding.DecodeString(encodeStr)
	assert.NoError(t, err)
	var s string
	err = json.Unmarshal(decodeByte, &s)
	assert.NoError(t, err)
	assert.Equal(t, str, s)
}

func TestChan(t *testing.T) {
	s := []string{"a", "b", "c"}
	sByte, err := json.Marshal(s)
	assert.NoError(t, err)
	var a []string
	err = json.Unmarshal(sByte, &a)
	assert.NoError(t, err)
}

func TestBase64Msg(t *testing.T) {
	msg := "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"
	dataBytes, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		fmt.Println("err1:", err)
		return
	}
	msgObj := &msgstream.InsertMsg{}
	tsMsg, err := msgObj.Unmarshal(dataBytes)
	if err != nil {
		fmt.Println("err2:", err)
		return
	}
	for _, data := range tsMsg.(*msgstream.InsertMsg).GetFieldsData() {
		fmt.Println(data.FieldName)
		if data.GetFieldName() == "$meta" {
			fmt.Println("len1:", len(data.GetScalars().GetStringData().GetData()))
			fmt.Println("len2:", len(data.GetScalars().GetLongData().GetData()))
		}
	}

	var columns []entity.Column
	for _, fieldData := range tsMsg.(*msgstream.InsertMsg).FieldsData {
		if column, err := entity.FieldDataColumn(fieldData, 0, -1); err == nil {
			columns = append(columns, column)
		} else {
			column, err := entity.FieldDataVector(fieldData)
			if err != nil {
				return
			}
			columns = append(columns, column)
		}
	}
	for _, column := range columns {
		fmt.Println(column.Name(), "-", column.Len())
	}
}
