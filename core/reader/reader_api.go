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

package reader

// import (
// 	"context"
//
// 	"github.com/zilliztech/milvus-cdc/core/model"
// )
//
// type CDCReader interface {
// 	StartRead(ctx context.Context) <-chan *model.CDCData
// 	QuitRead(ctx context.Context)
// }
//
// // DefaultReader All CDCReader implements should combine it
// type DefaultReader struct{}
//
// // StartRead the return value is nil,
// // and if you receive the data from the nil chan, will block forever, not panic
// func (d *DefaultReader) StartRead(ctx context.Context) <-chan *model.CDCData {
// 	log.Warn("StartRead is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultReader) QuitRead(ctx context.Context) {
// }
