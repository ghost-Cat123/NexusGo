package eval

import (
	"encoding/json"
	"fmt"
	"os"
)

// EvalQuery 标注数据集中的单条查询
type EvalQuery struct {
	ID             string   `json:"id"`
	Query          string   `json:"query"`
	TargetUser     string   `json:"target_user"`
	TargetGroup    string   `json:"target_group"`
	Keywords       string   `json:"keywords"`
	Topic          string   `json:"topic"`
	Difficulty     string   `json:"difficulty"`
	ExpectedMsgIDs []int64  `json:"expected_msg_ids"`
	Description    string   `json:"description"`
}

// EvalDataset 完整评测数据集
type EvalDataset struct {
	Queries []EvalQuery `json:"queries"`
}

// LoadDataset 从 JSON 文件加载评测数据集
func LoadDataset(path string) (*EvalDataset, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取数据集文件失败: %w", err)
	}

	var queries []EvalQuery
	if err := json.Unmarshal(data, &queries); err != nil {
		return nil, fmt.Errorf("解析数据集 JSON 失败: %w", err)
	}
	return &EvalDataset{Queries: queries}, nil
}
