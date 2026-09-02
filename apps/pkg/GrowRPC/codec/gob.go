package codec

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"io"
	"log"
)

// gobHeader 是 Gob 编解码专用的 wire 类型
// 使用独立类型而非直接编码 codec.Header，避免 Gob 类型缓存导致的
// "type mismatch" 问题（同一进程中多次 decode 时字段布局冲突）
// Metadata 序列化为 JSON 字符串嵌入，兼容 map[string]string 的 Gob 限制
type gobHeader struct {
	ServiceMethod string
	Seq           uint64
	Error         string
	Metadata      string // map[string]string 序列化为 JSON string
}

type GobCodec struct {
	// 链接实例
	conn io.ReadWriteCloser
	// 缓冲write
	buf *bufio.Writer
	// 信息编码器
	dec *gob.Decoder
	// 信息解码器
	enc *gob.Encoder
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (g GobCodec) Close() error {
	return g.conn.Close()
}

func (g GobCodec) ReadHeader(h *Header) error {
	var wh gobHeader
	if err := g.dec.Decode(&wh); err != nil {
		return err
	}
	h.ServiceMethod = wh.ServiceMethod
	h.Seq = wh.Seq
	h.Error = wh.Error
	// 还原 Metadata
	if wh.Metadata != "" {
		h.Metadata = make(map[string]string)
		if err := json.Unmarshal([]byte(wh.Metadata), &h.Metadata); err != nil {
			log.Println("rpc codec: gob decode metadata error:", err)
		}
	}
	return nil
}

func (g GobCodec) ReadBody(body interface{}) error {
	if body == nil {
		var discard interface{}
		return g.dec.Decode(&discard)
	}
	return g.dec.Decode(body)
}

func (g GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = g.buf.Flush()
		if err != nil {
			_ = g.Close()
		}
	}()

	// 序列化 Metadata 为 JSON 字符串
	metaStr := ""
	if len(h.Metadata) > 0 {
		b, _ := json.Marshal(h.Metadata)
		metaStr = string(b)
	}

	wh := gobHeader{
		ServiceMethod: h.ServiceMethod,
		Seq:           h.Seq,
		Error:         h.Error,
		Metadata:      metaStr,
	}

	if err := g.enc.Encode(&wh); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := g.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
