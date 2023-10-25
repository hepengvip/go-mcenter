package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"mcenter.io/gomcenter"

	proto "github.com/hepengvip/mcenter-proto"
)

const (
	DEFAULT_ADDR    = "localhost:8111"
	DEFAULT_CHANNEL = "default"
)

func main() {

	var userId, channels, addr string
	flag.StringVar(&userId, "uid", "", "userId")
	flag.StringVar(&addr, "addr", DEFAULT_ADDR, "server address")
	flag.StringVar(&channels, "channels", DEFAULT_CHANNEL, "channels, e.g. channel1,channel2,channel3")
	flag.Parse()

	channelNames := strings.Split(channels, ",")
	if channelNames[0] == "" {
		channelNames[0] = DEFAULT_CHANNEL
	}

	if userId == "" {
		fmt.Fprintf(os.Stderr, "need userId\n")
		return
	}

	cli := gomcenter.NewMCenterClient(userId, addr)
	reply, err := cli.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Received reply: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stdout, "Received reply: [%s]%d:%s\n", reply.ReqId, reply.ReqCode, reply.ReqMsg)

	ch := make(chan proto.Message, 100)
	rCh := make(chan proto.Response, 100)
	done := make(chan struct{})

	for idx, channel := range channelNames {
		fmt.Println("Sub:", idx, channel)
		cli.NewChannel(channel, fmt.Sprintf("newChan-%d", idx+1))
		cli.Subscribe(channel, fmt.Sprintf("subChan-%d", idx+1))
	}

	go BeginReader(cli, ch, rCh, done)

	BeginWriter(cli, channelNames[0])
}

func BeginWriter(cli *gomcenter.MCenterClient, channel string) {
	ticker := time.NewTicker(time.Second * 1)
	var idx uint64 = 0
	for range ticker.C {
		data := rand.Uint64()
		msg := fmt.Sprintf("B%d:%dE", idx, data)
		cli.Publish([]byte(msg), channel, fmt.Sprintf("index_%d", idx))
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
