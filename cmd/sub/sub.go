package main

import (
	"fmt"
	"os"

	proto "github.com/hepengvip/mcenter-proto"
	"mcenter.io/gomcenter"
)

const (
	ADDR = "localhost:8111"
	CHAN = "default"
	USER = "sub01"
)

func main() {
	cli := gomcenter.NewConsumer(ADDR)
	err := cli.Connect(USER)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	err = cli.NewChannel(CHAN, "declare_default_channel")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create channel failed: %v\n", err)
		os.Exit(2)
	}

	err = cli.Subscribe(CHAN, "subscribe_default_channel")
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe channel failed: %v\n", err)
		os.Exit(3)
	}

	for {
		msg, err := cli.NextMessage()
		if err != nil {
			fmt.Fprintf(os.Stderr, "get next message failed: %v\n", err)
			break
		}
		if msg.ReqType == proto.MSG_MESSAGE {
			fmt.Fprintf(os.Stdout, "Received message: %s, from: %s/%s\n", string(*msg.Payload), msg.UserId, msg.Channel)
		} else if msg.ReqType == proto.MSG_REPLY {
			reply, _ := msg.ToReply()
			fmt.Fprintf(os.Stdout, "Received reply: [%s]%d:%s\n", reply.ReqId, reply.ReqCode, reply.ReqMsg)
		} else {
			fmt.Fprintf(os.Stderr, "wrong message type: %s\n", msg.ReqType)
			continue
		}
	}
}
