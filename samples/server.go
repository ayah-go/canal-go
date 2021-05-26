package main

import (
	"fmt"
	"github.com/ayah/canal-go/client"
	pbe "github.com/ayah/canal-go/protocol/entry"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"time"
)

type CanalChannel struct {
	ChannelSql chan string
	Stop       bool
}

func StartCanal(address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32, regex string) *CanalChannel {

	result := &CanalChannel{
		ChannelSql: make(chan string),
		Stop:       false,
	}
	go func() {
		defer func() {
			fmt.Println("退出sql消费")
		}()
		// 创建链接
		connector := client.NewSimpleCanalConnector(address, port, username, password, destination, soTimeOut, idleTimeOut)
		err := connector.Connect()
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		// filter
		err = connector.Subscribe(regex)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		// 阻塞
		for {
			if result.Stop {
				break
			}
			message, err := connector.Get(100, nil, nil)
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}
			batchId := message.Id
			if batchId == -1 || len(message.Entries) <= 0 {
				time.Sleep(300 * time.Millisecond)
				//fmt.Println("===没有数据了===")
				continue
			}
			sql := GetSql(message.Entries)
			result.ChannelSql <- sql
		}
	}()

	return result
}

func GetSql(entrys []pbe.Entry) string {
	sql := ""
	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		eventType := rowChange.GetEventType()
		header := entry.GetHeader()
		for _, rowData := range rowChange.GetRowDatas() {
			if eventType == pbe.EventType_DELETE {
				keyColName := ""
				keyColValue := ""
				for _, col := range rowData.GetBeforeColumns() {
					if col.IsKey {
						keyColName = "`" + col.Name + "`"
						keyColValue = "'" + col.Value + "'"
						break
					}
				}
				rowChange.Sql += fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = %s ;\n", header.GetSchemaName(), header.GetTableName(), keyColName, keyColValue)
			} else if eventType == pbe.EventType_INSERT {
				colName := ""
				colValue := ""
				for index, col := range rowData.GetAfterColumns() {
					if index != len(rowData.GetAfterColumns())-1 {
						colName += "`" + col.Name + "`" + ","
						colValue += "'" + col.Value + "'" + ","
					} else {
						colName += col.Name
						colValue += "'" + col.Value + "'"
					}
				}
				tempSql := fmt.Sprintf("INSERT INTO  `%s`.`%s` (%s) VALUES (%s)  ;\n", header.GetSchemaName(), header.GetTableName(), colName, colValue)
				rowChange.Sql += tempSql
			} else if eventType == pbe.EventType_UPDATE {
				colChange := ""
				keyColName := ""
				keyColValue := ""
				for _, col := range rowData.GetAfterColumns() {
					if col.Updated {
						colChange += "`" + col.Name + "`" + "=" + "'" + col.Value + "'" + ","
					}
					if col.IsKey {
						keyColName = "`" + col.Name + "`"
					}
				}
				colChange = colChange[0 : len(colChange)-1]
				tempSql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s=%s ;\n", header.GetSchemaName(), header.GetTableName(), colChange, keyColName, keyColValue)
				rowChange.Sql += tempSql
			} else {

			}
		}
		sql += rowChange.Sql
	}
	return sql
}
