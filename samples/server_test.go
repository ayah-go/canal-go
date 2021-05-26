package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"testing"
)

func TestServer(t *testing.T) {

	ChannelSql := StartCanal("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000, ".*")
	// 启动消费
	count := 0

	go func() {
		for {
			if sql, ok := <-ChannelSql.ChannelSql; ok {
				count++
				fmt.Println("获取到sql：", count, "：", sql)
			}
			if count > 5 {
				ChannelSql.Stop = true
			}
		}
	}()

	engine := gin.Default()
	engine.Run("127.0.0.1:8888")

}
