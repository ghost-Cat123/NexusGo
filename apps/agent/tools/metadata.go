package tools

// 把工具分成两组 需要中断和只读工具
const (
	metaReadOnly    = "im:readOnly"
	metaDestructive = "im:destructive"
)

// 中断工具
var destructiveToolNames = map[string]bool{
	"schedule_message": true,
}

// 只读工具
var readOnlyToolNames = map[string]bool{
	"search_chat_history": true,
	"summarize_group":     true,
}

// IsDestructiveTool 返回是否是中断类型工具
func IsDestructiveTool(name string) bool {
	return destructiveToolNames[name]
}
