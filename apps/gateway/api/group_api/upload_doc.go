package group_api

import (
	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/proto/pb_group"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// AllowedDocExt 允许的文档类型
var AllowedDocExt = map[string]bool{
	".txt": true,
	".md":  true,
	// 后续扩展: ".pdf": true, ".docx": true
}

const maxDocSize = 10 << 20 // 10MB

// UploadDocHandler 上传群文档 (POST /api/group/:id/documents)
func UploadDocHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	groupIDStr := c.Param("id")
	groupID, err := strconv.ParseInt(groupIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "无效的群ID"})
		return
	}

	// 校验群存在（通过 RPC）
	uid := userID.(int64)
	info, err := rpcclient.GroupServiceClient.GetGroupInfo(c, &pb_group.GetGroupInfoArgs{GroupId: groupID})
	if err != nil || info == nil || info.GroupId == 0 {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "群不存在"})
		return
	}
	_ = uid // 后续可通过 info 校验成员身份

	// 接收文件
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "未找到上传文件"})
		return
	}
	defer file.Close()

	// 校验扩展名
	ext := strings.ToLower(filepath.Ext(header.Filename))
	if !AllowedDocExt[ext] {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 400,
			"msg":  fmt.Sprintf("不支持的文件类型 %s，仅支持: txt, md", ext),
		})
		return
	}

	// 校验大小
	if header.Size > maxDocSize {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "文件大小不能超过 10MB"})
		return
	}

	// 存储路径: uploads/groups/{groupID}/
	dir := filepath.Join("uploads", "groups", strconv.FormatInt(groupID, 10))
	if err := os.MkdirAll(dir, 0755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "创建存储目录失败"})
		return
	}

	// 文件名加时间戳避免冲突
	savedName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), header.Filename)
	dst := filepath.Join(dir, savedName)

	out, err := os.Create(dst)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "文件保存失败"})
		return
	}
	defer out.Close()

	if _, err = io.Copy(out, file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "文件写入失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "上传成功",
		"data": gin.H{
			"file_name":    header.Filename,
			"file_path":    dst,
			"file_size":    header.Size,
			"content_type": ext,
		},
	})
}
