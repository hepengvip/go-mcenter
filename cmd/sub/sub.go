package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"mcenter.io/gomcenter"

	proto "github.com/hepengvip/mcenter-proto"
)

func main() {
	cli := gomcenter.NewMCenterClient("sub01", "localhost:8111")
	reply, err := cli.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Received reply: [%s]%d:%s\n", reply.ReqId, reply.ReqCode, reply.ReqMsg)
		return
	}

	ch := make(chan proto.Message, 100)
	rCh := make(chan proto.Response, 100)
	done := make(chan struct{})

	cli.NewChannel("socket", "xsc01")
	cli.NewChannel("default", "xsc02")
	cli.Subscribe("default", "sub_def")

	go BeginReader(cli, ch, rCh, done)

	BeginWriter(cli)
}

func BeginWriter(cli *gomcenter.MCenterClient) {
	ticker := time.NewTicker(time.Second * 1)
	var idx uint64 = 0
	for range ticker.C {
		data := rand.Uint64()
		msg := fmt.Sprintf("B%d:%dE", idx, data)
		cli.Publish([]byte(msg), "default", fmt.Sprintf("index_%d", idx))
		idx += 1
	}
}

func BeginReader(cli *gomcenter.MCenterClient, ch chan proto.Message, rCh chan proto.Response, done chan struct{}) {
	cli.BeginReadMessage(ch, rCh)
	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(os.Stdout, "Received message: %s, from: %s/%s\n", string(*msg.Payload), msg.UserId, msg.Channel)
		case reply := <-rCh:
			fmt.Fprintf(os.Stdout, "Received reply: [%s]%d:%s\n", reply.ReqId, reply.ReqCode, reply.ReqMsg)
		case <-done:
			return
		}
	}
}
