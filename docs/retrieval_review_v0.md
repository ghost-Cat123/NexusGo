# 检索测评集 V0 审核说明

这是一份待审核的仿真 IM 消息检索集，不是已冻结的盲测集。

- 语料：36 条，均为 NexusGo 已实现能力的仿真聊天表述。
- 查询：48 条，其中 44 条有初标相关消息，4 条预期无结果。
- 类别：12 条精确术语、20 条语义改写、10 条多条件、4 条无答案、2 条多相关消息。

请优先审核 [expanded_retrieval_review_v0.csv](../apps/agent/eval/testdata/expanded_retrieval_review_v0.csv)：

1. `initial_expected_msg_ids` 是否遗漏了同等相关的消息。
2. `no_result` 是否确实应返回空结果。
3. 把 `review_status` 改为 `approved` 或 `revise`，在 `reviewer_note` 写明需改的 ID 或原因。

审核完成后，将 CSV 中的结论同步到 [expanded_retrieval_review_v0.json](../apps/agent/eval/testdata/expanded_retrieval_review_v0.json)，把版本冻结为 `v1.0`，再运行真实 Embedding 测评。冻结前不报告任何策略指标。
