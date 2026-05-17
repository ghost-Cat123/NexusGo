/**
 * ws_true_qps.js — 真实双人互发 QPS 压测脚本 v3 (修复 SyncUnread 污染)
 *
 * v2 核心修复：
 *   1. 每次压测生成唯一 RUN_ID，嵌入消息体
 *   2. Receiver 过滤掉不属于本轮测试的消息（SyncUnread 历史消息、其他轮次消息）
 *   3. 这样延迟数据才是真实的本轮 E2E 延迟，不会被历史积压消息污染
 *
 * 用法示例（账号需提前用 gen_users.go 生成）：
 *   # 50 对用户，每 100ms 一条 → 目标 500 msg/s（轻压，找健康延迟基线）
 *   k6 run -e VUS=50 -e SEND_INTERVAL=100 -e DURATION=60s perf/k6/ws_true_qps.js
 *
 *   # 50 对用户，每 70ms 一条 → 目标 ~700 msg/s（中压，逼近极限）
 *   k6 run -e VUS=50 -e SEND_INTERVAL=70 -e DURATION=60s perf/k6/ws_true_qps.js
 *
 *   # 100 对用户，每 100ms 一条 → 目标 1000 msg/s（扩容验证）
 *   k6 run -e VUS=100 -e SEND_INTERVAL=100 -e DURATION=60s perf/k6/ws_true_qps.js
 *
 * 账号规划：
 *   VUS=N 时需要 N*2 个账号，user_id = 1001 ~ 1000+N*2
 */

import { Counter, Rate, Trend } from "k6/metrics";
import { login, openWS } from "./common.js";

// ── 指标定义 ──────────────────────────────────────────────────────────────
const msgSent    = new Counter("true_msg_sent");    // sender 发出的消息总数
const msgRecv    = new Counter("true_msg_recv");    // receiver 本轮实际收到的消息数
const msgSkipped = new Counter("true_msg_skipped"); // 被过滤掉的历史/外轮消息数
const sendOK     = new Rate("true_send_ok");        // server_ack 到达率
const recvOK     = new Rate("true_recv_ok");        // 接收成功率
const e2eLatency = new Trend("true_e2e_ms");        // 端到端延迟（sender发出 → receiver收到）

// ── 测试参数 ──────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    true_qps: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 50) * 2, // 每对用户占 2 个 VU（sender + receiver）
      duration: __ENV.DURATION || "60s",
      // ★ 关键：给正在运行的 VU 足够时间完成，防止 k6 在 duration 结束前强制中断
      gracefulStop: "10s",
    },
  },
  thresholds: {
    true_send_ok: ["rate>0.99"],
    true_recv_ok: ["rate>0.99"],
    true_e2e_ms:  ["p(95)<500"],
  },
};

// ── Setup：批量登录 + 生成本轮唯一 RUN_ID ────────────────────────────────
export function setup() {
  const pairs = Number(__ENV.VUS || 50);

  // 生成本轮唯一 RUN_ID（6位随机字母数字），用于区分本轮消息与历史消息
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let runId = "R";
  for (let i = 0; i < 6; i++) {
    runId += chars[Math.floor(Math.random() * chars.length)];
  }

  console.log(`[Setup] ===== 本轮 RUN_ID: ${runId} =====`);
  console.log(`[Setup] 正在为 ${pairs} 对用户登录，共 ${pairs * 2} 个账号...`);

  const userPool = [];
  for (let i = 0; i < pairs; i++) {
    const senderID   = 1000 + i * 2 + 1;
    const receiverID = 1000 + i * 2 + 2;
    const senderToken   = login(senderID,   "test_user_" + senderID,   "123456");
    const receiverToken = login(receiverID, "test_user_" + receiverID, "123456");
    userPool.push({ senderID, senderToken, receiverID, receiverToken });
  }

  console.log(`[Setup] 登录完成，${pairs} 对账号就绪，开始压测！`);
  return { userPool, runId };
}

// ── 核心逻辑 ──────────────────────────────────────────────────────────────
export default function (data) {
  const { userPool, runId } = data;

  // 奇数 VU → sender，偶数 VU → receiver
  const pairIndex = Math.floor((__VU - 1) / 2);
  const isSender  = __VU % 2 === 1;
  const myPair    = userPool[pairIndex];

  if (!myPair) {
    console.error(`[VU-${__VU}] 找不到对应的账号对（pairIndex=${pairIndex}），跳过`);
    return;
  }

  const sendIntervalMs  = Number(__ENV.SEND_INTERVAL || 100);
  // ★ 修复核心：DURATION_MS 必须 > DURATION（60s），让 socket 撑满整个测试，
  //   防止 VU 在 58s 关闭 socket 后，剩余 2s 内发起第二次迭代触发 SyncUnread，
  //   导致历史消息被误计入 true_msg_recv、延迟虚高到 30-60s。
  const durationMs     = Number(__ENV.DURATION_MS   || 62000);

  // 消息格式（不做 JSON 序列化，receiver 用正则提取，避免高 QPS 下 k6 JS 引擎卡死）：
  //   msgTs:{发送时间戳}:rid:{RUN_ID}:p{对序号}:s{自增序号}
  // 示例：msgTs:1716444123456:rid:Rabc123:p3:s42
  const MSG_PATTERN = new RegExp(`msgTs:(\\d+):rid:${runId}:`);

  if (isSender) {
    // ── Sender 角色 ──────────────────────────────────────────────────────
    let localSent = 0;

    openWS(myPair.senderToken, function (socket) {
      socket.on("open", function () {
        socket.setInterval(function () {
          const now   = Date.now();
          const msgId = `msgTs:${now}:rid:${runId}:p${pairIndex}:s${localSent}`;

          socket.send(JSON.stringify({
            chat_type: "single_chat",
            receiver:  myPair.receiverID, // 真实发给另一个用户
            message:   msgId,
          }));

          localSent++;
          msgSent.add(1);
        }, sendIntervalMs);
      });

      socket.on("message", function (msg) {
        // sender 只关心 server_ack，有 ack = 消息成功进 MQ
        if (msg.includes("server_ack")) {
          sendOK.add(1);
        }
      });

      socket.on("error", function (e) {
        sendOK.add(0);
        console.error(`[Sender VU-${__VU}] WS 错误: ${e}`);
      });

      socket.setTimeout(function () { socket.close(); }, durationMs);
    });

  } else {
    // ── Receiver 角色 ─────────────────────────────────────────────────────
    openWS(myPair.receiverToken, function (socket) {
      socket.on("message", function (msg) {
        // ★ 核心过滤：只处理包含本轮 RUN_ID 的消息
        //   SyncUnread 推过来的历史消息、其他轮次的消息，全部忽略
        const match = msg.match(MSG_PATTERN);
        if (!match) {
          // 是历史消息或 server_ack 等非目标消息，直接跳过
          msgSkipped.add(1);
          return;
        }

        // 提取发送时间戳，计算真实 E2E 延迟
        const sentAt = parseInt(match[1], 10);
        const latency = Date.now() - sentAt;
        e2eLatency.add(latency);
        msgRecv.add(1);
        recvOK.add(1);
      });

      socket.on("error", function (e) {
        recvOK.add(0);
        console.error(`[Receiver VU-${__VU}] WS 错误: ${e}`);
      });

      socket.setTimeout(function () { socket.close(); }, durationMs);
    });
  }
}
