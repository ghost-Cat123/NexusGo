/**
 * ws_ai_stream.js — AI 流式响应压测脚本
 *
 * 核心指标:
 *   ai_first_chunk_ms  : TTFT（Time To First Token），即首字响应时间
 *   ai_total_stream_ms : 全量流完成时间
 *   ai_chunk_count     : 本次流式输出的 chunk 总数
 *   ai_done_ok         : 是否收到完整 ai_end 结束符
 *   ai_start_ok        : WS 连接+消息发送是否成功
 *
 * 环境变量:
 *   VUS          — 并发 VU 数 (default: 2，不建议超过 2，保护钱包防 429)
 *   DURATION     — 持续时长 (default: 60s)
 *   AI_WAIT_MS   — 单次等待 AI 响应的超时时间 (default: 30000ms)
 *   SLEEP_S      — 每轮结束后的冷却时间 (default: 5s)，防触发限流
 *   AI_QUESTION  — 发给 AI 的问题 (default: 见下方)
 *   GATEWAY_BASE / WS_BASE — 见 common.js
 *
 * 运行示例:
 *   k6 run perf/k6/ws_ai_stream.js
 *   k6 run -e VUS=1 -e DURATION=120s -e AI_WAIT_MS=45000 perf/k6/ws_ai_stream.js
 */

import { Counter, Rate, Trend } from "k6/metrics";
import { sleep } from "k6";
import { login, openWS } from "./common.js";

// ── 自定义指标 ──────────────────────────────────────────────────────────────
const aiStartOK       = new Rate("ai_start_ok");         // 消息发送成功率
const aiDoneOK        = new Rate("ai_done_ok");           // 流完整接收率
const aiChunkCount    = new Counter("ai_chunk_count");    // chunk 总计数
const firstChunkMs    = new Trend("ai_first_chunk_ms", true); // TTFT (ms)
const totalStreamMs   = new Trend("ai_total_stream_ms", true); // 全流耗时 (ms)
const aiTimeoutCount  = new Counter("ai_timeout_count"); // 超时次数
const aiErrorCount    = new Counter("ai_error_count");   // 解析/协议错误次数

// ── 场景配置 ────────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    ai_stream: {
      executor: "constant-vus",
      vus:      Number(__ENV.VUS || 2),
      duration: __ENV.DURATION || "60s",
    },
  },
  thresholds: {
    // 发送成功率 > 99%
    ai_start_ok:       ["rate>0.99"],
    // 流完整接收率 > 90%（AI 侧超时是常见外部噪声，此阈值可按实际调整）
    ai_done_ok:        ["rate>0.90"],
    // TTFT 95分位 < 5s（对话类 AI 可以适当放宽）
    ai_first_chunk_ms: ["p(95)<5000"],
    // 全流 95分位 < 30s
    ai_total_stream_ms:["p(95)<30000"],
  },
};

// ── Setup: 登录并缓存 Token ─────────────────────────────────────────────────
export function setup() {
  const vus = Number(__ENV.VUS || 2);
  const userPool = [];

  console.log(`[Setup] 准备 ${vus} 个 AI 测试账号...`);
  for (let i = 0; i < vus; i++) {
    const senderId   = 3 + i;
    const senderName = String.fromCharCode(97 + i); // 'a', 'b', ...
    const token      = login(senderId, senderName, "123456");
    userPool.push({ senderId, token });
    console.log(`[Setup] VU-${i + 1} 登录成功 (userId=${senderId})`);
  }
  return { userPool };
}

// ── 默认问题（可通过环境变量覆盖） ─────────────────────────────────────────
const AI_QUESTION = __ENV.AI_QUESTION ||
  "用一句话概括一下 Golang Goroutine 的核心调度原理";

// ── 主逻辑 ──────────────────────────────────────────────────────────────────
export default function (data) {
  const myData     = data.userPool[__VU - 1];
  const waitMs     = Number(__ENV.AI_WAIT_MS || 30000);

  // ── 每轮状态变量 ──
  const t0         = Date.now();
  let   sent       = false;
  let   gotFirst   = false;
  let   gotDone    = false;
  let   firstTs    = 0;
  let   chunkCount = 0;

  openWS(myData.token, function (socket) {

    // 1. 连接建立 → 立刻发送 AI 请求
    socket.on("open", function () {
      const payload = JSON.stringify({
        chat_type: "ai_chat",   // ★ 必须是 ai_chat，网关才会路由到 handlerAIChat
        receiver:  -1,
        message:   AI_QUESTION,
      });
      socket.send(payload);
      sent = true;
      console.log(`[VU-${__VU}] 已发送 AI 请求，等待首字... (timeout=${waitMs}ms)`);
    });

    // 2. 处理服务端下推的消息
    socket.on("message", function (raw) {
      let obj;
      try {
        obj = JSON.parse(raw);
      } catch (_) {
        aiErrorCount.add(1);
        console.warn(`[VU-${__VU}] 消息 JSON 解析失败: ${raw}`);
        return;
      }

      if (!obj || !obj.chat_type) return;

      switch (obj.chat_type) {
        case "ai_chunk":
          chunkCount++;
          aiChunkCount.add(1);
          if (!gotFirst) {
            gotFirst = true;
            firstTs  = Date.now();
            const ttft = firstTs - t0;
            firstChunkMs.add(ttft);
            console.log(`[VU-${__VU}] ★ 首字到达！TTFT = ${ttft} ms`);
          }
          break;

        case "ai_end":
          gotDone = true;
          const total = Date.now() - t0;
          totalStreamMs.add(total);
          console.log(
            `[VU-${__VU}] 流结束，总耗时=${total}ms，共${chunkCount}个chunk，` +
            `TTFT=${gotFirst ? (firstTs - t0) + "ms" : "N/A（未收到任何chunk）"}`
          );
          socket.close();
          break;

        case "server_ack":
          // 服务端已确认收到消息，不影响流状态
          console.log(`[VU-${__VU}] 收到 server_ack，msg_id=${obj.msg_id}`);
          break;

        default:
          // 忽略 chat_push / pong 等无关消息
          break;
      }
    });

    // 3. 超时处理
    socket.setTimeout(function () {
      if (!gotDone) {
        const elapsed = Date.now() - t0;
        aiTimeoutCount.add(1);
        totalStreamMs.add(elapsed);

        if (!gotFirst) {
          // 超时且未收到任何 chunk：最严重，大概率是后端/AI服务异常
          console.error(
            `[VU-${__VU}] ✗ 超时！等待 ${elapsed}ms 后未收到首字。` +
            `检查：1) Agent SSE 服务是否正常 2) chat_type 路由是否命中 ai_chat`
          );
        } else {
          // 收到了首字但流未结束：AI 生成过程中断
          console.warn(
            `[VU-${__VU}] ⚠ 超时！已收到首字（TTFT=${firstTs - t0}ms），` +
            `但 ai_end 未到，共收 ${chunkCount} 个 chunk`
          );
        }
      }
      socket.close();
    }, waitMs);

  }); // openWS

  // ── 记录最终指标 ──
  aiStartOK.add(sent);
  aiDoneOK.add(gotDone);

  // 冷却，防止 Redis 防刷屏限流被触发
  sleep(Number(__ENV.SLEEP_S || 5));
}