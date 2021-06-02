package canal

import (
	"fmt"
	"github.com/ayah-go/canal-go/client"
	pbe "github.com/ayah-go/canal-go/protocol/entry"
	"github.com/golang/protobuf/proto"
	"log"
	"strings"
	"time"
)

type Sql struct {
	Content string
	Type    string
	Insert  string
}

type Channel struct {
	ChannelSql chan []Sql
	Stop       bool
}

func RunCanalClient(address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32, regex string) *Channel {

	result := &Channel{
		ChannelSql: make(chan []Sql),
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
			//log.Println(err)
			log.Println("canal连接失败：", err)
			//os.Exit(1)
			return
		}
		log.Println("canal连接成功")
		// filter
		err = connector.Subscribe(regex)
		if err != nil {
			//log.Println(err)
			log.Println("canal订阅失败：", err)
			//os.Exit(1)
			return
		}
		log.Println("canal订阅成功")
		// 阻塞
		for {
			if result.Stop {
				break
			}
			message, err := connector.Get(100, nil, nil)
			if err != nil {
				log.Println("canal get获取失败：", err)
				//os.Exit(1)
				return
			}
			batchId := message.Id
			if batchId == -1 || len(message.Entries) <= 0 {
				time.Sleep(300 * time.Millisecond)
				//fmt.Println("===没有数据了===")
				continue
			}
			sqls := GetSql(message.Entries)
			if len(sqls) == 0 {
				continue
			}
			result.ChannelSql <- sqls
		}
	}()

	return result
}

/*
FormatColName 格式化列名 使用反引号包围
*/
func FormatColName(col *pbe.Column) string {
	return "`" + col.Name + "`"
}

/*
FormatColValue 格式化列数据 除去int bit类型的都要使用单引号包围
*/
func FormatColValue(col *pbe.Column) string {
	if strings.Index(col.MysqlType, "int") > -1 || strings.Index(col.MysqlType, "bit") > -1 {
		if col.Value == "" {
			return "null"
		}
		return col.Value
	} else {
		if col.Value == "" {
			return "null"
		}
		return "'" + col.Value + "'"
	}
}

func FormatSql(cols []*pbe.Column, isUpdate bool) (keyColName string, keyColValue string, colNames string, colValues string, updateChanges string) {
	for index, col := range cols {
		if col.IsKey {
			keyColName = FormatColName(col)
			keyColValue = FormatColValue(col)
		}
		if index != len(cols)-1 {
			colNames += FormatColName(col) + ","
			colValues += FormatColValue(col) + ","
			if isUpdate && col.Updated {
				// 更新需要拼接为 col=value,
				updateChanges += FormatColName(col) + "=" + FormatColValue(col) + ","
			}
		} else {
			colNames += FormatColName(col)
			colValues += FormatColValue(col)
			if isUpdate && col.Updated {
				// 更新需要拼接为 col=value
				updateChanges += FormatColName(col) + "=" + FormatColValue(col)
			}
		}

	}
	return
}

func GetSql(entrys []pbe.Entry) []Sql {
	var sqls []Sql
	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			fmt.Println("出现异常", err)
			return nil
		}
		eventType := rowChange.GetEventType()
		header := entry.GetHeader()
		for _, rowData := range rowChange.GetRowDatas() {
			if eventType == pbe.EventType_DELETE {
				// 删除
				keyColName, keyColValue, _, _, _ := FormatSql(rowData.GetBeforeColumns(), false)
				tempSql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = %s ;\n", header.GetSchemaName(), header.GetTableName(), keyColName, keyColValue)
				sqls = append(sqls, Sql{Content: tempSql, Type: "DELETE"})
			} else if eventType == pbe.EventType_INSERT {
				// 插入
				_, _, colNames, colValues, _ := FormatSql(rowData.GetAfterColumns(), false)
				tempSql := fmt.Sprintf("INSERT INTO  `%s`.`%s` (%s) VALUES (%s)  ;\n", header.GetSchemaName(), header.GetTableName(), colNames, colValues)
				sqls = append(sqls, Sql{Content: tempSql, Type: "INSERT"})
			} else if eventType == pbe.EventType_UPDATE {
				// 更新
				keyColName, keyColValue, _, _, colChange := FormatSql(rowData.GetAfterColumns(), true)
				if keyColName == "" || keyColValue == "" {
					keyColName, keyColValue, _, _, _ = FormatSql(rowData.GetBeforeColumns(), true)
				}
				tempSql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s=%s ;\n", header.GetSchemaName(), header.GetTableName(), colChange, keyColName, keyColValue)
				// 同时给出insert语句
				_, _, colNames, colValues, _ := FormatSql(rowData.GetAfterColumns(), false)
				insertSql := fmt.Sprintf("INSERT INTO  `%s`.`%s` (%s) VALUES (%s)  ;\n", header.GetSchemaName(), header.GetTableName(), keyColName+","+colNames, keyColValue+","+colValues)
				sqls = append(sqls, Sql{Content: tempSql, Insert: insertSql, Type: "UPDATE"})
			} else {

			}
		}
		if rowChange.Sql != "" {
			sqls = append(sqls, Sql{Content: rowChange.Sql + ";\n", Type: "ALTER"})
		}
	}
	return sqls
}
