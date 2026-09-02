package eval

import (
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/logger"
	vdb "NexusGo/apps/pkg/vector_db"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if err := config.InitConfig(""); err != nil {
		fmt.Printf("config init failed: %v\n", err)
		os.Exit(1)
	}
	logger.InitLogger()
	defer logger.Log.Sync()

	if config.GlobalConfig == nil {
		fmt.Println("config.GlobalConfig is nil")
		os.Exit(1)
	}
	dbErr := db.InitMySQL(config.GlobalConfig.MySQL)
	if dbErr != nil {
		fmt.Printf("DB init failed: %v\n", dbErr)
	} else {
		fmt.Println("DB init succeeded")
	}
	// 初始化 Milvus（用于语义检索评测）
	// 本地 go test 时 Milvus addr 需要改为 127.0.0.1:19530
	milvusCfg := config.GlobalConfig.Milvus
	if milvusCfg.Addr == "milvus:19530" {
		milvusCfg.Addr = "127.0.0.1:19530" // Docker 内部 hostname 本地测试不可用
	}
	if milvusErr := vdb.InitClient(milvusCfg); milvusErr != nil {
		fmt.Printf("Milvus init failed (Milvus search will be skipped): %v\n", milvusErr)
	} else {
		fmt.Println("Milvus init succeeded")
	}
	os.Exit(m.Run())
}

func TestRunEvaluation(t *testing.T) {
	if db.GetDB() == nil {
		t.Skip("DB not available, skipping eval tests")
	}

	wd, _ := os.Getwd()
	datasetPath := filepath.Join(wd, "testdata", "queries.json")
	if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
		datasetPath = filepath.Join(wd, "..", "..", "..", "apps", "agent", "eval", "testdata", "queries.json")
	}

	report, err := RunEvaluation(datasetPath)
	if err != nil {
		t.Fatalf("评测失败: %v", err)
	}

	output := report.Format()
	fmt.Println(output)

	if report.TotalQueries == 0 {
		t.Error("数据集为空")
	}
	if len(report.ByStrategy) == 0 {
		t.Error("没有策略评测结果")
	}

	for _, s := range report.ByStrategy {
		if s.Count == 0 {
			t.Errorf("策略 %s 没有执行任何查询", s.Name)
		}
		t.Logf("策略 %s: R@5=%.2f MRR=%.2f NDCG@5=%.2f", s.Name, s.RecallAt5, s.MRR, s.NDCGAt5)
	}
}
