package main

import (
	"fmt"
	"os"

	"mcenter.io/gomcenter"
)

const (
	ADDR    = "localhost:8111"
	CHANNEL = "default"
	USER    = "pub01"
)

func main() {

	cli := gomcenter.NewProducer(ADDR)
	err := cli.Connect(USER)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	err = cli.NewChannel(CHANNEL, CHANNEL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create channel failed: %v\n", err)
		os.Exit(2)
	}

	for idx := 0; idx < 10; idx++ {
		msg := fmt.Sprintf("message-%d", idx)
		reply, err := cli.PublishSync([]byte(msg), CHANNEL, msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "publish failed: %v\n", err)
			continue
		}
		fmt.Fprintf(os.Stdout, "Reply: %d:%s\n", reply.ReqCode, reply.ReqMsg)
	}
}
