package canal

import (
	"fmt"
	"testing"
)

func TestServer(t *testing.T) {

	ChannelSql := RunCanalClient("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000, ".*")
	// 启动消费
	count := 0

	go func() {
		for {
			if sql, ok := <-ChannelSql.ChannelSql; ok {
				count++
				fmt.Println(count, "获取到sql：", count, "：", sql)
			}
		}
	}()

	select {}
}
