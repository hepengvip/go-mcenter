package gomcenter

import (
	"bufio"
	"fmt"
	"net"

	proto "github.com/hepengvip/mcenter-proto"
)

type MCenterClient struct {
	MsgType proto.Message
	addr    string
	userId  string
	conn    *net.Conn
	reader  *bufio.Reader
}

func NewMCenterClient(userId, addr string) *MCenterClient {
	return &MCenterClient{
		addr:   addr,
		userId: userId,
	}
}

func (m *MCenterClient) Connect() (*proto.Response, error) {
	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		return nil, err
	}

	m.conn = &conn
	m.reader = bufio.NewReader(conn)

	return m.setUser()
}

func (m *MCenterClient) writeBytes(data []byte) error {
	_, err := (*m.conn).Write(data)
	return err
}

func (m *MCenterClient) readHeader() (*proto.Message, error) {
	header, err := proto.ReadHeader(m.reader, '\n')
	if err != nil {
		return nil, err
	}
	msg, err := proto.Parse(*header)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *MCenterClient) setUser() (*proto.Response, error) {
	msg := proto.Message{
		ReqType: proto.MSG_SET_USER,
		UserId:  m.userId,
	}

	m.writeBytes(msg.ToBytes())

	headerMsg, err := m.readHeader()
	if err != nil {
		return nil, err
	}

	if headerMsg.ReqType == proto.MSG_REPLY {
		reply, _ := headerMsg.ToReply()
		return reply, nil
	}
	return nil, fmt.Errorf("unknown response type")
}

func (m *MCenterClient) Publish(data []byte, channel, reqId string) error {
	msg := proto.Message{
		ReqType:     proto.MSG_PUBLISH,
		ReqId:       reqId,
		Channel:     channel,
		PayloadSize: len(data),
		Payload:     &data,
	}

	return m.writeBytes(msg.ToBytes())
}

func (m *MCenterClient) NewChannel(channel, reqId string) error {
	msg := proto.Message{
		ReqType: proto.MSG_NEW_CHANNEL,
		ReqId:   reqId,
		Channel: channel,
	}
	fmt.Println("###debug", string(msg.ToBytes()))

	return m.writeBytes(msg.ToBytes())
}

func (m *MCenterClient) Subscribe(channel, reqId string) error {
	msg := proto.Message{
		ReqType: proto.MSG_SUBSCRIBE,
		ReqId:   reqId,
		Channel: channel,
	}

	return m.writeBytes(msg.ToBytes())
}

func (m *MCenterClient) Unsubscribe(channel, reqId string) error {
	msg := proto.Message{
		ReqType: proto.MSG_UNSUBSCRIBE,
		ReqId:   reqId,
		Channel: channel,
	}

	return m.writeBytes(msg.ToBytes())
}

func (m *MCenterClient) BeginReadMessage(ch chan proto.Message, rCh chan proto.Response) {

	go func(chan proto.Message, chan proto.Response) {

		for {
			msg, err := m.readHeader()
			if err != nil {
				fmt.Printf("%v\n", err)
				break
			}

			// if msg.ReqType == proto.MSG_REPLY {
			// 	reply, _ := msg.ToReply()
			// 	fmt.Printf("Header: %s\n", string(reply.ToBytes()))
			// } else {
			// 	fmt.Printf("Header: %s\n", string(msg.ToBytes()))
			// }

			switch msg.ReqType {
			case proto.MSG_MESSAGE:
				err := msg.ReadPayload(m.reader)
				if err != nil {
					break
				}
				ch <- *msg
			case proto.MSG_REPLY:
				reply, _ := msg.ToReply()
				rCh <- *reply
			}
		}
	}(ch, rCh)
}
