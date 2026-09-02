package codec

import "io"

// 消息编解码相关代码

type Header struct {
	// 服务名和方法名 对应go的结构图和方法
	ServiceMethod string
	// 请求ID 区分请求
	Seq uint64
	// 服务端错误信息
	Error string
	// 用于传输 Context 数据
	Metadata map[string]string
}

// Codec 对消息体编解码的结构
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType      Type = "application/gob"
	JsonType     Type = "application/json"
	ProtobufType Type = "application/protobuf"
)

// NewCodecFuncMap 根据解析的形式创建不同构造函数 类似于工厂模式
var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
	NewCodecFuncMap[JsonType] = NewJsonCodec
	NewCodecFuncMap[ProtobufType] = NewProtobufCodec
}
