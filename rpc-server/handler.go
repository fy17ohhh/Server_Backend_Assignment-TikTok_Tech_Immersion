package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

type Message struct {
	Sender    string `json:"sender"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	timestamp := time.Now().Unix()
	message := &Message{
		Message:   req.Message.GetText(),
		Sender:    req.Message.GetSender(),
		Timestamp: timestamp,
	}

	roomID, err := getRoomID(req.Message.GetChat())
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", "im-assignment:tiktok2023@tcp(localhost:3306)/")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	query := "INSERT INTO chat(roomID, message) VALUES (?, ?)"
	insertResult, err := db.ExecContext(context.Background(), query, roomID, message)
	if err != nil {
		log.Fatalf("impossible insert: %s", err)
	}
	id, err := insertResult.LastInsertId()
	if err != nil {
		log.Fatalf("impossible to retrieve last inserted id: %s", err)
	}

	log.Printf("inserted id: %d", id)

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	roomID, err := getRoomID(req.GetChat())
	if err != nil {
		return nil, err
	}

	limit := int64(req.GetLimit())
	if limit == 0 {
		limit = 10 // default limit 10
	}
	start := req.GetCursor()
	end := start + limit // did not minus 1 on purpose for hasMore check later on

	db, err := sql.Open("mysql", "im-assignment:tiktok2023@tcp(localhost:3306)/")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	log.Printf("Getting messages by roomID")
	query := `select message, timestamp from chat where roomID == ?;`
	ctx, CancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer CancelFunc()

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, roomID)
	defer rows.Close()

	var messages = []Message{}
	for rows.Next() {
		var msg Message
		messages = append(messages, msg)
	}

	respMessages := make([]*rpc.Message, 0)
	var counter int64 = 0
	var nextCursor int64 = 0
	hasMore := false
	for _, msg := range messages {
		if counter+1 > limit {
			// having extra value here means it has more data
			hasMore = true
			nextCursor = end
			break // do not return the last message
		}
		temp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}
		respMessages = append(respMessages, temp)
		counter += 1
	}

	resp := rpc.NewPullResponse()
	resp.Messages = respMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil
}

func validateSendRequest(req *rpc.SendRequest) error {
	senders := strings.Split(req.Message.Chat, ":")
	if len(senders) != 2 {
		err := fmt.Errorf("invalid Chat ID '%s', should be in the format of user1:user2", req.Message.GetChat())
		return err
	}
	sender1, sender2 := senders[0], senders[1]

	if req.Message.GetSender() != sender1 && req.Message.GetSender() != sender2 {
		err := fmt.Errorf("sender '%s' not in the chat room", req.Message.GetSender())
		return err
	}

	return nil
}

func getRoomID(chat string) (string, error) {
	var roomID string

	lowercase := strings.ToLower(chat)
	senders := strings.Split(lowercase, ":")
	if len(senders) != 2 {
		err := fmt.Errorf("invalid Chat ID '%s' %s, should be in the format of user1:user2", chat, senders)
		return "", err
	}

	sender1, sender2 := senders[0], senders[1]
	// Compare the sender and receiver alphabetically, and sort it asc to form the room ID
	if comp := strings.Compare(sender1, sender2); comp == 1 {
		roomID = fmt.Sprintf("%s:%s", sender2, sender1)
	} else {
		roomID = fmt.Sprintf("%s:%s", sender1, sender2)
	}

	return roomID, nil
}
