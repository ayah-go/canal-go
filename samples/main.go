// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions anda
// limitations under the License.

package main

//
//import (
//	"fmt"
//	"github.com/gin-gonic/gin"
//	"log"
//	"os"
//	"time"
//
//	"github.com/ayah-go/canal-go/client"
//	pbe "github.com/ayah-go/canal-go/protocol/entry"
//	"github.com/golang/protobuf/proto"
//)
//
//func main() {
//
//	ChannelSql := StartCanal("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000, ".*")
//	// 启动消费
//	count := 0
//
//	go func() {
//		for {
//			if sql, ok := <-ChannelSql.ChannelSql; ok {
//				count++
//				fmt.Println("获取到sql：", count, "：", sql)
//			}
//			if count > 5 {
//				ChannelSql.Stop = true
//			}
//		}
//	}()
//
//	engine := gin.Default()
//	engine.Run("127.0.0.1:8888")
//}
//
//func Test() {
//
//	// example 替换成-e canal.destinations=example 你自己定义的名字
//	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000)
//	err := connector.Connect()
//	if err != nil {
//		log.Println(err)
//		os.Exit(1)
//	}
//
//	// https://github.com/alibaba/canal/wiki/AdminGuide
//	//mysql 数据解析关注的表，Perl正则表达式.
//	//
//	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
//	//
//	//常见例子：
//	//
//	//  1.  所有表：.*   or  .*\\..*
//	//	2.  canal schema下所有表： canal\\..*
//	//	3.  canal下的以canal打头的表：canal\\.canal.*
//	//	4.  canal schema下的一张表：canal\\.test1
//	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
//
//	err = connector.Subscribe(".*")
//	if err != nil {
//		log.Println(err)
//		os.Exit(1)
//	}
//
//	for {
//
//		message, err := connector.Get(100, nil, nil)
//		if err != nil {
//			log.Println(err)
//			os.Exit(1)
//		}
//		batchId := message.Id
//		if batchId == -1 || len(message.Entries) <= 0 {
//			time.Sleep(300 * time.Millisecond)
//			//fmt.Println("===没有数据了===")
//			continue
//		}
//		sql := printEntry(message.Entries)
//		fmt.Println(sql)
//
//	}
//}
//
//func printEntry(entrys []pbe.Entry) string {
//	sql := ""
//	for _, entry := range entrys {
//		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
//			continue
//		}
//		rowChange := new(pbe.RowChange)
//
//		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
//		checkError(err)
//		eventType := rowChange.GetEventType()
//		header := entry.GetHeader()
//		for _, rowData := range rowChange.GetRowDatas() {
//			if eventType == pbe.EventType_DELETE {
//				keyColName := ""
//				keyColValue := ""
//				for _, col := range rowData.GetBeforeColumns() {
//					if col.IsKey {
//						keyColName = "`" + col.Name + "`"
//						keyColValue = "'" + col.Value + "'"
//						break
//					}
//				}
//				rowChange.Sql += fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = %s ;\n", header.GetSchemaName(), header.GetTableName(), keyColName, keyColValue)
//			} else if eventType == pbe.EventType_INSERT {
//				printColumn(rowData.GetAfterColumns())
//				colName := ""
//				colValue := ""
//				for index, col := range rowData.GetAfterColumns() {
//					if index != len(rowData.GetAfterColumns())-1 {
//						colName += "`" + col.Name + "`" + ","
//						colValue += "'" + col.Value + "'" + ","
//					} else {
//						colName += col.Name
//						colValue += "'" + col.Value + "'"
//					}
//				}
//				tempSql := fmt.Sprintf("INSERT INTO  `%s`.`%s` (%s) VALUES (%s)  ;\n", header.GetSchemaName(), header.GetTableName(), colName, colValue)
//				rowChange.Sql += tempSql
//			} else if eventType == pbe.EventType_UPDATE {
//				printColumn(rowData.GetAfterColumns())
//				colChange := ""
//				keyColName := ""
//				keyColValue := ""
//				for _, col := range rowData.GetAfterColumns() {
//					if col.Updated {
//						colChange += "`" + col.Name + "`" + "=" + "'" + col.Value + "'" + ","
//					}
//					if col.IsKey {
//						keyColName = "`" + col.Name + "`"
//					}
//				}
//				colChange = colChange[0 : len(colChange)-1]
//				tempSql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s=%s ;\n", header.GetSchemaName(), header.GetTableName(), colChange, keyColName, keyColValue)
//				rowChange.Sql += tempSql
//			} else {
//				fmt.Println("-------> before")
//				printColumn(rowData.GetBeforeColumns())
//				fmt.Println("-------> after")
//				printColumn(rowData.GetAfterColumns())
//			}
//		}
//		sql += rowChange.Sql
//	}
//	return sql
//}
//
//func printColumn(columns []*pbe.Column) {
//	for _, col := range columns {
//		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
//	}
//}
//
//func checkError(err error) {
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
//		os.Exit(1)
//	}
//}
