/**
 * ws_group_chat.js — 群聊写扩散压测脚本
 *
 * 核心目标：量化「写扩散」对系统的影响
 *   - 单聊：1 条消息 → 1 次 DB 写入
 *   - 群聊（N 人群）：1 条消息 → N 次 DB 写入（写放大 = N 倍）
 *
 * 前置条件（必须手动执行 seed_group.sql 预置数据）：
 *   - groups 表：group_id=9001, group_name='perf_group_1', owner_id=2001
 *   - group_members 表：group_id=9001，成员 2001~2010（10 人）
 *   - 账号 2001~2010 已存在于 users 表，密码均为 123456
 *
 * 用法：
 *   # 10 人群，1 个 sender，9 个 receiver，每 200ms 一条消息
 *   k6 run -e VUS=10 -e GROUP_ID=9001 -e SEND_INTERVAL=200 -e DURATION=60s perf/k6/ws_group_chat.js
 *
 *   # 对比单聊（ws_true_qps.js VUS=1），观察写放大对延迟的影响
 *   k6 run -e VUS=10 -e GROUP_ID=9001 -e SEND_INTERVAL=100 -e DURATION=30s perf/k6/ws_group_chat.js
 *
 * 关键指标：
 *   group_e2e_ms      端到端延迟（sender 发出 → 某个 receiver 收到）
 *   group_recv_ok     接收成功率
 *   group_msg_sent    发送总数
 *   group_msg_recv    接收总数（理论 = 发送数 × (N-1)，N 为群人数）
 *   group_fanout      实际写放大倍数（recv / sent）
 */

import { Counter, Rate, Trend } from "k6/metrics";
import { login, openWS } from "./common.js";

// ── 自定义指标 ─────────────────────────────────────────────────────────────
const msgSent    = new Counter("group_msg_sent");   // sender 发出条数
const msgRecv    = new Counter("group_msg_recv");   // 所有 receiver 收到的总条数
const msgSkipped = new Counter("group_msg_skipped");// 过滤掉的非本轮消息
const sendOK     = new Rate("group_send_ok");       // server_ack 到达率
const recvOK     = new Rate("group_recv_ok");       // 接收成功率
const e2eLatency = new Trend("group_e2e_ms");       // 端到端延迟（毫秒）

// ── 测试参数 ───────────────────────────────────────────────────────────────
export const options = {
  scenarios: {
    group_chat: {
      executor:     "constant-vus",
      // VUS = 群成员数（第 1 个 VU 是 sender，其余是 receiver）
      vus:          Number(__ENV.VUS || 10),
      duration:     __ENV.DURATION || "60s",
      gracefulStop: "10s",
    },
  },
  thresholds: {
    group_send_ok: ["rate>0.99"],
    group_recv_ok: ["rate>0.95"],   // 群聊写扩散允许少量丢失
    group_e2e_ms:  ["p(95)<800"],   // 群聊因写扩散延迟比单聊高
  },
};

// ── Setup：批量登录群成员账号 ────────────────────────────────────────────
export function setup() {
  const groupID  = Number(__ENV.GROUP_ID  || 9001);
  const memberCount = Number(__ENV.VUS    || 10);
  // 账号 ID 从 BASE_USER_ID 开始，连续 memberCount 个
  const baseUID  = Number(__ENV.BASE_USER_ID || 2001);

  // 生成本轮唯一 RUN_ID，过滤 SyncUnread 历史消息污染
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let runId = "G";
  for (let i = 0; i < 6; i++) {
    runId += chars[Math.floor(Math.random() * chars.length)];
  }

  console.log(`[Setup] ===== 群聊压测 RUN_ID: ${runId} =====`);
  console.log(`[Setup] 群 ID: ${groupID}，成员数: ${memberCount}，账号范围: ${baseUID}~${baseUID + memberCount - 1}`);

  const members = [];
  for (let i = 0; i < memberCount; i++) {
    const uid   = baseUID + i;
    const uname = "test_user_" + uid;
    const token = login(uid, uname, "123456");
    members.push({ uid, token });
    console.log(`[Setup] 账号 ${uid} 登录成功`);
  }

  console.log(`[Setup] ${memberCount} 个群成员登录完成，开始压测！`);
  return { members, groupID, runId };
}

// ── 核心逻辑 ───────────────────────────────────────────────────────────────
export default function (data) {
  const { members, groupID, runId } = data;

  // VU-1 是 sender（第一个成员），其余都是 receiver
  // __VU 从 1 开始：__VU=1 → sender，__VU=2~N → receiver
  const myIndex   = __VU - 1;
  const isSender  = __VU === 1;
  const myMember  = members[myIndex % members.length];

  if (!myMember) {
    console.error(`[VU-${__VU}] 找不到对应成员账号，跳过`);
    return;
  }

  const sendIntervalMs = Number(__ENV.SEND_INTERVAL || 200);
  const durationMs     = Number(__ENV.DURATION_MS   || 62000);

  // 消息格式：msgTs:{时间戳}:rid:{RUN_ID}:g{群ID}:s{序号}
  // 用正则提取时间戳，避免高 QPS 下 JSON.parse 导致 k6 卡死
  const MSG_PATTERN = new RegExp(`msgTs:(\\d+):rid:${runId}:`);

  if (isSender) {
    // ── Sender：定时往群里发消息 ──────────────────────────────────────────
    let localSent = 0;

    openWS(myMember.token, function (socket) {
      socket.on("open", function () {
        console.log(`[Sender VU-${__VU}] WS 连接成功，开始向群 ${groupID} 发消息`);

        socket.setInterval(function () {
          const now   = Date.now();
          const msgId = `msgTs:${now}:rid:${runId}:g${groupID}:s${localSent}`;

          socket.send(JSON.stringify({
            chat_type: "group_chat",
            group_id:  groupID,   // 群消息必须带 group_id
            message:   msgId,
          }));

          localSent++;
          msgSent.add(1);
        }, sendIntervalMs);
      });

      socket.on("message", function (msg) {
        // sender 只关心 server_ack
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
    // ── Receiver：监听群消息，计算延迟 ──────────────────────────────────
    openWS(myMember.token, function (socket) {
      socket.on("open", function () {
        console.log(`[Receiver VU-${__VU}] WS 连接成功，监听群 ${groupID} 消息`);
      });

      socket.on("message", function (msg) {
        // 过滤：只统计包含本轮 RUN_ID 的群消息
        const match = msg.match(MSG_PATTERN);
        if (!match) {
          msgSkipped.add(1);
          return;
        }

        // 提取发送时间戳计算 E2E 延迟
        const sentAt  = parseInt(match[1], 10);
        const latency = Date.now() - sentAt;
        e2eLatency.add(latency);
        msgRecv.add(1);
        recvOK.add(1);

        // 可选：打印样本延迟（低频抽样，避免日志爆炸）
        if (msgRecv.count % 50 === 0) {
          console.log(`[Receiver VU-${__VU}] 第 ${msgRecv.count} 条，延迟 ${latency}ms`);
        }
      });

      socket.on("error", function (e) {
        recvOK.add(0);
        console.error(`[Receiver VU-${__VU}] WS 错误: ${e}`);
      });

      socket.setTimeout(function () { socket.close(); }, durationMs);
    });
  }
}

// ── Teardown：打印写放大倍数（informational only） ─────────────────────
export function teardown(data) {
  // 注意：k6 不支持在 teardown 读取 Counter 当前值，此处仅作提示
  console.log("=== 群聊压测完成 ===");
  console.log(`理论写放大倍数 = 群人数 - 1 = ${Number(__ENV.VUS || 10) - 1}`);
  console.log("实际放大倍数 = group_msg_recv / group_msg_sent（见 k6 summary 输出）");
}
