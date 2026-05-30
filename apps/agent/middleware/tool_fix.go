package middleware

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/cloudwego/eino/compose"
	"github.com/kaptinlin/jsonrepair"
)

// ToolFixMiddleware bundles both invokable and streamable wrappers for convenience.
func ToolFixMiddleware() compose.ToolMiddleware {
	return compose.ToolMiddleware{Invokable: Invokable, Streamable: Streamable}
}

// Invokable 非流式工具调用
func Invokable(next compose.InvokableToolEndpoint) compose.InvokableToolEndpoint {
	return func(ctx context.Context, in *compose.ToolInput) (*compose.ToolOutput, error) {
		in.Arguments = repair(in.Arguments)
		return next(ctx, in)
	}
}

// Streamable 流式工具调用
func Streamable(next compose.StreamableToolEndpoint) compose.StreamableToolEndpoint {
	return func(ctx context.Context, in *compose.ToolInput) (*compose.StreamToolOutput, error) {
		in.Arguments = repair(in.Arguments)
		return next(ctx, in)
	}
}

// repair attempts minimal work first (validity check, region isolation) and
// only uses jsonrepair when necessary. It trims common LLM artifacts.
func repair(input string) string {
	s := strings.TrimSpace(input)

	// 尽力而为，提取第一个有效JSON对象
	if obj := extractFirstObject(s); obj != "" && json.Valid([]byte(obj)) {
		return obj
	}

	// 快速路径：合法JSON直接放行
	if json.Valid([]byte(s)) {
		if strings.HasPrefix(s, "{") {
			return s
		}
		if strings.HasPrefix(s, "[") {
			s = extractFirstObject(s)
			if s != "" {
				return s
			}
		}
	}

	// 提取{ }之间的string进行合法性检查
	i := strings.IndexByte(s, '{')
	j := strings.LastIndexByte(s, '}')
	if i >= 0 && j >= i {
		sub := s[i : j+1]
		if json.Valid([]byte(sub)) {
			return sub
		}
		s = sub
	}

	// 去除[]数组包裹
	if strings.HasPrefix(s, "[") {
		obj := extractFirstObject(s)
		if obj != "" {
			s = obj
		}
	}

	// 去除LLM生成的提示
	s = strings.TrimPrefix(s, "<|FunctionCallBegin|>")
	s = strings.TrimSuffix(s, "<|FunctionCallEnd|>")
	s = strings.TrimPrefix(s, "<think>")

	// 判断是否合法
	if json.Valid([]byte(s)) {
		return s
	}
	// 如果最终还是不合法
	if !strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		s = "{" + s
	} else if strings.HasPrefix(s, "{") && !strings.HasSuffix(s, "}") {
		s = s + "}"
	}
	// 尝试修复
	out, err := jsonrepair.Repair(s)
	if err != nil {
		// Hard fallback: extract first object from original input
		if obj := extractFirstObject(input); obj != "" {
			return obj
		}
		return s
	}

	// Post-repair: if result is still an array, extract first object
	if strings.HasPrefix(out, "[") {
		obj := extractFirstObject(out)
		if obj != "" {
			return obj
		}
	}

	return out
}

// extractFirstObject 检查左右{}匹配，根据{找到匹配的}，返回之间的string
func extractFirstObject(s string) string {
	start := strings.IndexByte(s, '{')
	if start < 0 {
		return ""
	}
	// 记录
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		// 有一个{就加一个
		case '{':
			depth++
		// 没有就消掉一个
		case '}':
			depth--
			// 最终为0则是匹配的右括号
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}
