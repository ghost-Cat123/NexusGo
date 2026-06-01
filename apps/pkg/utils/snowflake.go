package utils

import (
	"NexusGo/apps/pkg/logger"
	"fmt"
	"sync"
	"time"
)

var (
	snowflakeInstance *Snowflake
	once              sync.Once
)

// Snowflake 雪花算法
type Snowflake struct {
	mu            sync.Mutex
	lastTimestamp int64
	sequence      int64
	nodeID        int64
}

// InitSnowflake 初始化雪花算法（单例模式）
func InitSnowflake(nodeID int64) error {
	var initErr error
	once.Do(func() {
		if nodeID < 0 || nodeID > 1023 {
			initErr = fmt.Errorf("node ID must be between 0 and 1023")
			return
		}
		snowflakeInstance = &Snowflake{
			nodeID: nodeID,
		}
		logger.Log.Infof("Snowflake initialized with nodeID: %d", nodeID)
	})
	return initErr
}

// GetSnowflake 获取雪花算法实例
func GetSnowflake() *Snowflake {
	if snowflakeInstance == nil {
		panic("snowflake not initialized, call InitSnowflake first")
	}
	return snowflakeInstance
}

// NewSnowflake 创建新的雪花算法实例（非单例，用于测试）
func NewSnowflake(nodeID int64) *Snowflake {
	if nodeID < 0 || nodeID > 1023 {
		panic("node ID must be between 0 and 1023")
	}
	return &Snowflake{
		nodeID: nodeID,
	}
}

// Generate 生成唯一ID
func (s *Snowflake) Generate() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := time.Now().UnixMilli()

	if current < s.lastTimestamp {
		// 时钟回拨，等待
		for current <= s.lastTimestamp {
			time.Sleep(time.Millisecond)
			current = time.Now().UnixMilli()
		}
	}

	if current == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & 0xFFF // 12位序列号
		if s.sequence == 0 {
			// 同一毫秒内序列号用完，等待下一毫秒
			for current <= s.lastTimestamp {
				time.Sleep(time.Millisecond)
				current = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = current

	// 组成64位ID: 时间戳(41位) + 节点ID(10位) + 序列号(12位)
	return (current << 22) | (s.nodeID << 12) | s.sequence
}

// GenerateString 生成字符串格式的ID（便于存储和传输）
func (s *Snowflake) GenerateString() string {
	return fmt.Sprintf("%d", s.Generate())
}

type SnowflakeInfo struct {
	Timestamp int64
	NodeID    int64
	Sequence  int64
}

// ParseSnowflakeID 解析雪花ID
func ParseSnowflakeID(id int64) SnowflakeInfo {
	return SnowflakeInfo{
		Timestamp: (id >> 22) + 1288834974657, // 加上起始时间戳
		NodeID:    (id >> 12) & 0x3FF,         // 10位节点ID
		Sequence:  id & 0xFFF,                 // 12位序列号
	}
}
