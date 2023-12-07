/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
)

const (
	ResSuccess              = int16(0)
	ResError                = int16(1)
	ResFlushDiskTimeout     = int16(10)
	ResSlaveNotAvailable    = int16(11)
	ResFlushSlaveTimeout    = int16(12)
	ResServiceNotAvailable  = int16(14)
	ResNoPermission         = int16(16)
	ResTopicNotExist        = int16(17)
	ResPullNotFound         = int16(19)
	ResPullRetryImmediately = int16(20)
	ResPullOffsetMoved      = int16(21)
	ResQueryNotFound        = int16(22)
)

type SendMessageResponse struct {
	MsgId         string
	QueueId       int32
	QueueOffset   int64
	TransactionId string
	MsgRegion     string
}

func (response *SendMessageResponse) Decode(properties map[string]string) {

}

type PullMessageResponse struct {
	SuggestWhichBrokerId int64
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
}

type GetKVConfigResponseHeader struct {
	Value string
}

func (header *GetKVConfigResponseHeader) Decode(properties map[string]string) {
	header.Value = properties["value"]
}

type SyncCountResponseHeader struct {
	Count int
}

func (header *SyncCountResponseHeader) Decode(properties map[string]string) {
	header.Count, _ = strconv.Atoi(properties["count"])
}

type ClusterInfo struct {
	BrokerAddrTable  map[string]BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string   `json:"clusterAddrTable"`
}

func (info *ClusterInfo) Decode(data string) error {
	res := gjson.Parse(data)
	info.ClusterAddrTable = make(map[string][]string)
	err := jsoniter.Unmarshal([]byte(res.Get("clusterAddrTable").String()), &info.ClusterAddrTable)
	if err != nil {
		return err
	}

	bds := res.Get("brokerAddrTable").Map()
	info.BrokerAddrTable = make(map[string]BrokerData)
	for k, v := range bds {
		bd := &BrokerData{
			BrokerName:      v.Get("brokerName").String(),
			Cluster:         v.Get("cluster").String(),
			BrokerAddresses: make(map[int64]string, 0),
		}
		addrs := v.Get("brokerAddrs").String()
		strs := strings.Split(addrs[1:len(addrs)-1], ",")
		if strs != nil {
			for _, str := range strs {
				i := strings.Index(str, ":")
				if i < 0 {
					continue
				}
				brokerId := strings.ReplaceAll(str[0:i], "\"", "")
				id, _ := strconv.ParseInt(brokerId, 10, 64)
				bd.BrokerAddresses[id] = strings.Replace(str[i+1:], "\"", "", -1)
			}
		}
		info.BrokerAddrTable[k] = *bd
	}
	return nil
}
