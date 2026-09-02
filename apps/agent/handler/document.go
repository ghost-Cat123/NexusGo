package handler

import (
	"NexusGo/apps/agent/graph"
	"NexusGo/apps/pkg/config"
	vdb "NexusGo/apps/pkg/vector_db"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino/schema"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// IndexDocumentReq 文档索引请求
type IndexDocumentReq struct {
	FilePath string `json:"file_path" binding:"required"`
	GroupID  int64  `json:"group_id" binding:"required"`
	FileName string `json:"file_name" binding:"required"`
}

// IndexDocumentHandler Agent 内部端点：解析文件 → 切片 → Embed → 写入 Milvus
// POST /agent/documents/index
func IndexDocumentHandler(c *gin.Context) {
	var req IndexDocumentReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	// 1. 读取文件
	content, err := os.ReadFile(req.FilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "文件读取失败: " + err.Error()})
		return
	}
	if len(strings.TrimSpace(string(content))) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "文件内容为空"})
		return
	}

	// 2. 切片
	chunker := graph.NewFixedSizeChunker(512, 50)
	chunks := chunker.Chunk(string(content))

	// 3. Embed + 写入 Milvus
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "Embedder 初始化失败"})
		return
	}

	indexer, err := vdb.GetIndxer(c, vdb.GroupDocCollection, embedder, vdb.GroupDocConverter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "Indexer 初始化失败"})
		return
	}

	docs := make([]*schema.Document, 0, len(chunks))
	for i, chunk := range chunks {
		docs = append(docs, &schema.Document{
			ID:      uuid.NewString(),
			Content: chunk,
			MetaData: map[string]any{
				"group_id":    req.GroupID,
				"file_name":   req.FileName,
				"chunk_idx":   int64(i),
				"upload_time": time.Now().Unix(),
			},
		})
	}

	// 分批写入（embedding API 单次上限 10 条）
	const batchSize = 10
	total := 0
	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		if _, err := indexer.Store(c, docs[start:end]); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"code": 500, "msg": fmt.Sprintf("向量化失败 [%d:%d]: %v", start, end, err),
			})
			return
		}
		total += end - start
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "文档索引完成",
		"data": gin.H{"chunks": total, "size_bytes": len(content)},
	})
}
