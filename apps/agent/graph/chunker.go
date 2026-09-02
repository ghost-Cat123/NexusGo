package graph

// ── 文档切片器（摄入层，不属于查询管线）──
// Chunker 用于文件上传时将长文档切成适合 embedding 的片段
//
// 使用场景：
//   1. 用户上传 PDF/文本 → Gateway 接收文件
//   2. 文件内容传给 Chunker.Chunk() → 得到 []string
//   3. 每个 chunk embed → 写入 Milvus group_docs collection
//   4. 检索时按 chunk 粒度召回，Pipeline.Rerank/Execute 处理
//
// 注意：Chunker 是独立的摄入组件，不在 Pipeline 内部。
// Pipeline 负责查询时处理（Retrieve 之后的步骤），
// Chunker 负责摄入时处理（Store 之前的步骤）。

// FixedSizeChunker 固定大小切片器
// 按字符数切割，支持 overlap 防止上下文断裂
type FixedSizeChunker struct {
	ChunkSize    int // 每块最大字符数（默认 512）
	ChunkOverlap int // 相邻块重叠字符数（默认 50）
}

// NewFixedSizeChunker 创建固定大小切片器
func NewFixedSizeChunker(chunkSize, overlap int) *FixedSizeChunker {
	if chunkSize <= 0 {
		chunkSize = 512
	}
	if overlap < 0 || overlap >= chunkSize {
		overlap = 50
	}
	return &FixedSizeChunker{
		ChunkSize:    chunkSize,
		ChunkOverlap: overlap,
	}
}

// Chunk 实现 Chunker 接口
// 示例：content="ABCDEFGHIJ", chunkSize=4, overlap=1
//
//	→ ["ABCD", "DEFG", "GHIJ"]
func (c *FixedSizeChunker) Chunk(content string) []string {
	runes := []rune(content)
	if len(runes) <= c.ChunkSize {
		return []string{content}
	}

	var chunks []string
	step := c.ChunkSize - c.ChunkOverlap
	if step <= 0 {
		step = c.ChunkSize
	}

	for start := 0; start < len(runes); start += step {
		end := start + c.ChunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[start:end]))
		if end == len(runes) {
			break
		}
	}
	return chunks
}

// ── 未来可扩展的切片实现（示例注释）──
//
// SemanticChunker：用 embedding 相似度检测语义边界，在"话题转折点"切分
//   type SemanticChunker struct { embedder Embedder; threshold float64 }
//
// MarkdownChunker：按 ## 标题分段，保持文档结构
//   type MarkdownChunker struct { maxChunkSize int }
//
// 使用时直接替换实例即可：
//   chunker := NewSemanticChunker(embedder, 0.7)
//   chunks := chunker.Chunk(fileContent)
