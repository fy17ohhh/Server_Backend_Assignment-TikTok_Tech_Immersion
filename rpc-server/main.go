package main

import (
	"context"
	"database/sql"
	"fmt"
	rpc "github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc/imservice"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	etcd "github.com/kitex-contrib/registry-etcd"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	r, err := etcd.NewEtcdRegistry([]string{"etcd:2379"}) // r should not be reused.

	svr := rpc.NewServer(new(IMServiceImpl), server.WithRegistry(r), server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{
		ServiceName: "demo.rpc.server",
	}))

	err = svr.Run()
	if err != nil {
		log.Println(err.Error())
	}

	db, err := sql.Open("mysql", "im-assignment:tiktok2023@tcp(localhost:3306)/")
	if err != nil {
		log.Fatal(err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)

	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully connected to MySQL database")

	query := `CREATE TABLE IF NOT EXISTS chat(roomID string primary key, messageID int auto_increment,
	message text, timestamp datetime default CURRENT_TIMESTAMP)`

	ctx, CancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer CancelFunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating product table", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
	}
	log.Printf("Rows affected when creating table: %d", rows)

}
