package codec

import (
	"GrowRPC/codec/pb"
	"bufio"
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
)

type ProtobufCodec struct {
	// 链接实例
	conn io.ReadWriteCloser
	// 缓冲write
	buf *bufio.Writer
}

var _ Codec = (*ProtobufCodec)(nil)

func NewProtobufCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &ProtobufCodec{
		conn: conn,
		buf:  buf,
	}
}

func (p *ProtobufCodec) Close() error {
	return p.conn.Close()
}

func (p *ProtobufCodec) ReadHeader(h *Header) error {
	// RPC请求头 对应Header
	pbHeader := &pb.RequestHeader{}
	// 从网络流中读取信息，反序列化到pbHeader中
	if err := p.readMessage(pbHeader); err != nil {
		return err
	}
	// 将pbHeader（TCP）中的数据传递给Header请求头（RPC）
	h.ServiceMethod = pbHeader.ServiceMethod
	h.Seq = pbHeader.Seq
	h.Error = pbHeader.Error
	h.Metadata = pbHeader.Metadata

	return nil
}

func (p *ProtobufCodec) ReadBody(body interface{}) error {
	if body == nil {
		return p.readMessage(nil)
	}

	msg, ok := body.(proto.Message)
	if !ok {
		return fmt.Errorf("rpc codec: body must implement proto.Message")
	}
	return p.readMessage(msg)
}

func (p *ProtobufCodec) Write(h *Header, body interface{}) error {
	// 1. 构建pbHeader（TCP）请求头
	pbHeader := &pb.RequestHeader{
		ServiceMethod: h.ServiceMethod,
		Seq:           h.Seq,
		Error:         h.Error,
		Metadata:      h.Metadata,
	}

	// 2. 发送header
	if err := p.writeMessage(pbHeader); err != nil {
		return err
	}

	// 3. 发送body
	if body == nil {
		return p.writeEmptyBody()
	}

	// 发送的是空结构体 struct{}
	if _, ok := body.(struct{}); ok {
		return p.writeEmptyBody()
	}

	// 必须要类型断言 规定proto.Marshal必须传入msg
	msg, ok := body.(proto.Message)
	if !ok {
		return fmt.Errorf("rpc codec: body must implement proto.Message")
	}
	if err := p.writeMessage(msg); err != nil {
		return err
	}
	// 清空缓冲等待下一次发送
	return p.buf.Flush()
}

// 内部方法，分别对应Encoder和Decoder
func (p *ProtobufCodec) readMessage(msg proto.Message) error {
	// 自定义头部 4字节长度+数据 防止粘包
	// 1. 读出长度
	var length uint32
	if err := binary.Read(p.conn, binary.BigEndian, &length); err != nil {
		return err
	}

	// 2. 根据长度读取具体字节流
	data := make([]byte, length)
	if _, err := io.ReadFull(p.conn, data); err != nil {
		return err
	}

	// 3. 反序列化
	if msg == nil {
		return nil
	}
	return proto.Unmarshal(data, msg)
}

func (p *ProtobufCodec) writeMessage(msg proto.Message) error {
	// 反序列化
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// 1. 先写入长度
	length := uint32(len(data))
	if err := binary.Write(p.buf, binary.BigEndian, length); err != nil {
		return err
	}

	// 2. 再写入数据
	_, err = p.buf.Write(data)
	return err
}

// 写入一个 0 长度的数据包[0,0,0,0]，防止对端读取阻塞或越界
func (p *ProtobufCodec) writeEmptyBody() error {
	if err := binary.Write(p.buf, binary.BigEndian, uint32(0)); err != nil {
		return err
	}
	return p.buf.Flush()
}
