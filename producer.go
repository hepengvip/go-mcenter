package gomcenter

import (
	"bufio"
	"fmt"
	"net"

	proto "github.com/hepengvip/mcenter-proto"
)

type Producer struct {
	addr   string
	userId string
	conn   *net.Conn
	reader *bufio.Reader
}

func NewProducer(addr string) *Producer {
	return &Producer{
		addr: addr,
	}
}

func (m *Producer) Close() {
	(*m.conn).Close()
}

func (m *Producer) Connect(userId string) error {
	conn, err := net.Dial("tcp", m.addr)
	if err != nil {
		return err
	}

	m.conn = &conn
	m.reader = bufio.NewReader(conn)

	// send auth
	msg := proto.Message{
		ReqType: proto.MSG_SET_USER,
		UserId:  userId,
	}
	err = m.writeBytes(msg.ToBytes())
	if err != nil {
		m.Close()
		return err
	}

	// get reply
	reply, err := m.NextReply()
	if err != nil {
		m.Close()
		return err
	}

	if reply.ReqCode != 0 {
		m.Close()
		return fmt.Errorf("%d:%s", reply.ReqCode, reply.ReqMsg)
	}

	// success: set attrs
	m.userId = userId
	m.conn = &conn
	m.reader = bufio.NewReader(conn)
	return nil
}

func (m *Producer) writeBytes(data []byte) error {
	_, err := (*m.conn).Write(data)
	return err
}

func (m *Producer) readHeader() (*proto.Message, error) {
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

func (m *Producer) NextReply() (*proto.Response, error) {
	headerMsg, err := m.NextMessage()
	if err != nil {
		return nil, err
	}

	if headerMsg.ReqType != proto.MSG_REPLY {
		return nil, fmt.Errorf("not reply - %v", headerMsg.ToBytes())
	}

	return headerMsg.ToReply()
}

func (m *Producer) NextMessage() (*proto.Message, error) {
	msg, err := m.readHeader()
	if err != nil {
		return nil, err
	}

	if msg.ReqType == proto.MSG_MESSAGE {
		msg.ReadPayload(m.reader)
	}
	return msg, nil
}

func (m *Producer) Publish(data []byte, channel, reqId string) error {
	msg := proto.Message{
		ReqType:     proto.MSG_PUBLISH,
		ReqId:       reqId,
		Channel:     channel,
		PayloadSize: len(data),
		Payload:     &data,
	}

	return m.writeBytes(msg.ToBytes())
}

func (m *Producer) PublishSync(data []byte, channel, reqId string) (*proto.Response, error) {
	msg := proto.Message{
		ReqType:     proto.MSG_PUBLISH,
		ReqId:       reqId,
		Channel:     channel,
		PayloadSize: len(data),
		Payload:     &data,
	}

	err := m.writeBytes(msg.ToBytes())
	if err != nil {
		return nil, err
	}

	return m.NextReply()
}

func (m *Producer) NewChannel(channel, reqId string) error {
	msg := &proto.Message{
		ReqType: proto.MSG_NEW_CHANNEL,
		ReqId:   reqId,
		Channel: channel,
	}

	return m.writeBytes(msg.ToBytes())
}
