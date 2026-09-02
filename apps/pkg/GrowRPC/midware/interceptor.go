package midware

import (
	. "GrowRPC"
	"fmt"
	"log"
	"time"
)

// LoggerInterceptor 1. 日志与请求耗时拦截器
func LoggerInterceptor(next HandlerFunc) HandlerFunc {
	return func(i *CallInfo) error {
		start := time.Now()
		log.Printf("[RPC Call Start] Method: %s | Argv: %v", i.ServiceMethod, i.ReqArgs)
		err := next(i)
		log.Printf("[RPC Call End] Method: %s | Cost: %v | Error: %v", i.ServiceMethod, time.Since(start), err)
		return err
	}
}

func RecoveryInterceptor(next HandlerFunc) HandlerFunc {
	return func(i *CallInfo) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[RPC Panic Recovered] Method: %s | Panic: %v", i.ServiceMethod, r)
				err = fmt.Errorf("internal panic: %v", r)
			}
		}()
		err = next(i)
		// 命名返回值defer return 前修改返回值
		return err
	}
}
