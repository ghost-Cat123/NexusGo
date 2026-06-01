package logger

import (
	"NexusGo/apps/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

var Log *zap.SugaredLogger

func InitLogger() {
	logConf := config.GlobalConfig.Log
	// 1. 配置日志切割 lumberjack
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logConf.Filename,
		MaxSize:    logConf.MaxSize,
		MaxBackups: logConf.MaxBackups,
		MaxAge:     logConf.MaxAge,
		Compress:   logConf.Compress,
	}
	writeSyncer := zapcore.AddSync(lumberJackLogger)
	// 同时将日志输出到控制台和文件，方便开发调试
	multiWriteSyncer := zapcore.NewMultiWriteSyncer(writeSyncer, zapcore.AddSync(os.Stdout))

	// 2. 配置日志输出格式
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder   // 人类可读的时间格式，例如: 2026-04-16T10:21:07.000+0800
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // 级别大写，例如: INFO, ERROR
	encoder := zapcore.NewJSONEncoder(encoderConfig)

	// 3. 设置日志级别
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(logConf.Level)); err != nil {
		level = zapcore.InfoLevel // 兜底使用 Info
	}

	// 4. 生成 Core 并构建 Logger
	core := zapcore.NewCore(encoder, multiWriteSyncer, level)

	// AddCaller() 会在日志里自动打印出是哪一行代码打印的日志！极其方便找 Bug！
	logger := zap.New(core, zap.AddCaller())

	// 我们使用 SugaredLogger，因为它支持类似 fmt.Printf 的占位符语法，方便你改造旧代码
	Log = logger.Sugar()

	Log.Infof("✅ Zap 日志组件初始化成功，输出路径: %s", logConf.Filename)
}
