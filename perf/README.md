# 压测说明（k6）

本目录提供 3 个场景的统一压测脚本：

- `perf/k6/ws_online_chat.js`：在线单聊实时转发
- `perf/k6/ws_offline_sync.js`：离线堆积后上线同步
- `perf/k6/ws_ai_stream.js`：AI 流式输出（`ai_chunk`/`ai_end`）

## 1. 前置准备

1. 启动 `gateway`、`logic`、`mysql`、`redis`
2. 准备两个可登录账号（sender/receiver）
3. 安装 k6（Windows 可用 `choco install k6`）

> 默认网关地址：`http://127.0.0.1:8080` / `ws://127.0.0.1:8080`

## 2. 环境变量

如果你已有 token，直接传 `SENDER_TOKEN` / `RECEIVER_TOKEN`；否则脚本会用登录接口换 token。

必须提供（无 token 时）：

- `SENDER_ID` `SENDER_NAME` `SENDER_PASSWORD`
- `RECEIVER_ID` `RECEIVER_NAME` `RECEIVER_PASSWORD`（在线/离线场景）

通用可选：

- `GATEWAY_BASE`（默认 `http://127.0.0.1:8080`）
- `WS_BASE`（默认 `ws://127.0.0.1:8080`）
- `VUS`（并发用户）
- `DURATION`（如 `2m`）

## 3. 执行示例

### 3.1 在线单聊

```bash
k6 run perf/k6/ws_online_chat.js VUS=50 -e DURATION=2m
```

关键指标：

- `online_e2e_ms`：发送到接收端收到的端到端延迟
- `online_send_ok`、`online_receive_ok`：发送/接收成功率

### 3.2 离线同步

```bash
k6 run perf/k6/ws_offline_sync.js -e OFFLINE_BURST=10 -e VUS=10 -e DURATION=30s
```

关键指标：

- `offline_sync_ms`：离线写入 + 上线同步总耗时
- `offline_sync_ok`：同步成功率
- `offline_sync_count`：同步收到的离线消息数量

### 3.3 AI 流式输出

```bash
k6 run perf/k6/ws_ai_stream.js -e VUS=2 -e DURATION=30s
```

关键指标：

- `ai_first_chunk_ms`：首个 `ai_chunk` 到达延迟
- `ai_total_stream_ms`：整段流（到 `ai_end`）耗时
- `ai_done_ok`：`ai_end` 成功率

## 4. 指标采集建议

最小化方案（先出简历数字）：

- 直接保存 k6 结果：`k6 run ... --summary-export=perf/result_online.json`
- 同步记录机器配置：CPU/内存/部署拓扑（单机/双机）
- 记录业务侧错误率：gateway/logs 中 WS 写失败、RPC 错误

进阶方案（可视化）：

- k6 输出到 Prometheus Remote Write 或 InfluxDB
- 采集 MySQL（QPS、慢查询）、Redis（ops/s、内存、阻塞）、网关 CPU/GC

## 5. 自建 RPC 对延迟和丢包的影响

会影响，主要在这几个点：

1. **序列化/反序列化开销**：自定义协议未做零拷贝、压缩策略时，P95 延迟会明显上升。
2. **连接池与超时策略**：池过小/无退避重试会导致瞬时抖动和调用失败增多。
3. **背压与队列长度**：缺少请求队列和限流时，高峰会在 gateway->logic 链路放大尾延迟。
4. **错误处理语义**：若重试不幂等，可能出现“看起来像丢包”的重复/覆盖问题。

建议你在面试中这样描述：

- “当前自建 RPC 可用，但在高并发下尾延迟和失败率受连接池、超时与序列化策略影响；已通过 k6 压测量化 P95/P99，并计划引入更完善的可观测与限流机制。”

---

## 6. Ⱥ��д��ɢѹ��

### 6.1 ʲô��д��ɢ��ΪʲôҪ�⣿

Ⱥ�Ĳ���**д��ɢ**ģʽ��1 ����Ϣ �� N �� DB INSERT��д�Ŵ� = Ⱥ��������

ѹ��Ŀ�꣺����Ⱥ��ģ��ϵͳ���º��ӳٵ�Ӱ�죬�ҵ�д��ɢ�ļ��޵㡣

### 6.2 ǰ��׼����ִ�� SQL ������

```bash
# Docker ����
docker exec -i im-mysql mysql -u root -p123456 IMSystem < perf/seed_group.sql
```

ִ�к󴴽�Ⱥ group_id=9001��10 ����Ա user_id=2001~2010������� 123456����

### 6.3 ִ������

```bash
# ��׼��10 ��Ⱥ��ÿ 200ms һ��
k6 run -e VUS=10 -e GROUP_ID=9001 -e SEND_INTERVAL=200 -e DURATION=60s perf/k6/ws_group_chat.js

# ��ѹ��ÿ 100ms һ�����۲�д�Ŵ���ӳٵ�Ӱ�죩
k6 run -e VUS=10 -e GROUP_ID=9001 -e SEND_INTERVAL=100 -e DURATION=30s perf/k6/ws_group_chat.js
```

### 6.4 �ؼ�ָ��

| ָ�� | ���� |
|------|------|
| group_msg_sent | sender �������� |
| group_msg_recv | ���� receiver �յ��������� sent �� (N-1)�� |
| group_e2e_ms   | �˵����ӳ� |
| group_recv_ok  | ���ճɹ��ʣ���ֵ >95%�� |

### 6.5 ���Ի���

���ʡ�Ⱥ�ĺ͵�����������ʱ��

> Ⱥ����д��ɢ��10 ��Ⱥÿ����Ϣ���� 10 �� DB д�롣ѹ��Աȣ�10 ��Ⱥ P95 �ӳٱȵ��ĸ�Լ 50%��
> ƿ���� MySQL ����д�롣����Ⱥ���Կ��ǻ�����ɢ����һ����Ϣ����ѯʱ��չ����
> �� DB ���� DB д����д��ɢ vs ����ɢ�ľ���Ȩ�⡣
