# Bridge 反向访问（云端 → 本地，Reverse Access）

> 状态：设计稿（未实现）。前置阅读 [bridge.md](./bridge.md)。
> 范围：让被拨入侧（通常是云端）能经**同一条已建立的 Bridge WS** 反向发起受限调用，操作拨入侧（通常是 NAT 后的本地实例）的专家。实现仍落 `infra/bridge/`，凭证原语与 remote runtime 共用（§8）。

## 1. 问题与根因

现状体验单向：本地可以操作云端，云端无法调用本地。传输层并不是瓶颈 —— Bridge WS 与其上的隧道帧本身对称 —— 单向的根因在**凭证不对称**：

| 侧 | 连接行 | 持有凭证 | 能力 |
|----|--------|----------|------|
| 发起方（本地，NAT 后） | 出站行 | 对端用户的登录态（`credential_blob`） | probe / 重拨 / 经隧道操作对端 |
| 被拨入方（云端） | 入站行（`_is_inbound`：无 `credential_blob`） | **无** | 仅服务对方发来的隧道请求 |

配对流程（bridge.md §4.1）假定发起方能 HTTP 打到对端完成登录；NAT 场景下只有 本地→云端 一个方向可行，于是云端永远拿不到「能在本地证明自己」的身份。`_is_inbound`（`infra/bridge/manager.py`）明确：入站行无密码、不能重拨、不能 probe。

## 2. 目标 / 非目标

| 项 | 结论 |
|----|------|
| 目标 | 云端经既有 WS 反向调用本地：专家列表、对话、工作区读写 —— 与本地操作云端的能力镜像 |
| 凭证 | 配对时一次性签发 **bridge 专用 scoped key**（`bk_` 前缀），本地只存 hash；云端加密存明文。**绝不**让云端持有本地用户的密码 |
| 授权 | 本地侧连接级开关（默认关）+ path 白名单 + `allow_exec` 开关，可随时撤销 |
| 传输 | 复用同一条 Bridge WS 的隧道帧；不新增连接、不新开端口 |
| 非目标 | 不改变「对话在专家所在实例执行」模型；不取代 remote runtime enrollment（§8）；不支持浏览器直连对端；v1 不做反向 turn 流以外的 SSE 透传 |

## 3. 总体设计

```
配对（本地 → 云端，现有流程不变）：
  本地 login_peer(云端) 成功
  本地生成 bk_<ulid> scoped key，hash 存本地出站行
  hello 帧携带 reverse.offer { key, scopes, allow_exec }   ── 仅此一次明文出现
  云端校验后加密存入自己的入站行，回 hello_ack.reverse.accepted

反向调用（云端 → 本地，同一条 WS）：
  云端 dashboard 请求本机（bridge 反向目标）
  云端经隧道发 tunnel.request，Authorization: Bearer bk_…
  本地（dialer 侧）新增反向处理器：校验 key → 白名单 → 以受限 bridge 身份执行
  响应原路返回
```

## 4. 凭证模型

- 形态：`bk_<26 位 ULID 小写>`，仅握手帧中出现一次明文；与 runtime enrollment 的 `rk_` 同构。
- 本地（签发方）：`bridge_connections` 出站行新增 `reverse_key_hash`、`reverse_scopes`、`reverse_allow_exec`、`reverse_granted_at`。校验用常数时间比较。
- 云端（持有方）：入站行新增 `reverse_key_encrypted`（沿用现有 credential 加密通道存储）。
- 公共件：签发 / hash / 校验 / 撤销 / 轮换逻辑从 `runtime_enrollments` 抽为 `infra/security/scoped_keys.py`，bridge 反向与 runtime enrollment 共用（§8）。

## 5. 协议扩展（最小增量）

```text
→ hello          { …现有字段…, reverse_offer?: { key, scopes[], allow_exec, granted_at } }
← hello_ack      { …现有字段…, reverse_accepted: bool, reverse_reason? }
→ tunnel.request { id, method, path, query, headers, body_b64? | chunks, auth: "bk_<…>" }
← tunnel.response / tunnel.error   （形状不变；鉴权失败 tunnel.error { code: "reverse_auth_failed" } ）
```

- `reverse_offer` 只允许出现在**首次** hello；重连 hello 携带的是既有 key 的存在性声明（`reverse_key_hash_prefix`，前 8 位，供对端对账），不重复下发明文。
- `tunnel.request` 新增可选 `auth` 字段：dialer 侧收到带 `auth` 的请求即为**反向请求**，走反向处理器；不带 `auth` 的请求维持现状（仅 dialed 侧服务）。
- 帧方向不新增类型：多路复用层本就以 `id` 区分请求/响应，反向只是放开「dialed 侧也可发 request」。

## 6. 本地侧反向处理器（dialer 端）

1. `auth` 与出站行 `reverse_key_hash` 匹配（常数时间）；不匹配 → `tunnel.error reverse_auth_failed`，并计入连接级失败计数。
2. `reverse_scopes` + `allow_exec` 决定放行面（§7）。
3. 执行身份：**受限 bridge 服务身份**，不是本地任意用户。可见范围 = 配对时本地用户显式勾选的专家集合（默认：仅该用户自有专家）；不继承 admin。
4. 响应经隧道回云端，云端按 `bridge:{connection_id}:{agent_id}` 同一映射规则暴露给自己的 dashboard（与 §5.2 本机路由镜像对称）。

## 7. 白名单与授权面

- 反向允许的 path = 现有入站白名单的**镜像**：agents 列表/对话/工作区/定时任务/状态 + limited 工具（`tunnel_policy.py` 同源生成，避免两份清单漂移）。
- `allow_exec` 默认 `0`：为 1 时才放行 exec 类（`POST …/execute`、shell 通道）；与 runtime enrollment 的 `allow_exec` 语义一致。
- 管理 / 认证 / Bridge 控制面照旧不入隧道（双向皆然）。
- 拒绝语义：fail-closed —— key 失效、scopes 不含、连接撤销，一律显式错误，不静默降级。

## 8. 与 remote runtime 的关系：抽公共件，不合并系统

| | Bridge 反向访问 | Remote runtime enrollment |
|----|----------------|--------------------------|
| 对端 | 完整 Octop 实例 | 轻量 daemon（stdlib + websockets） |
| 专家在哪 | 拨入侧（本地），云端做代理 | 云端 hub，仅 fs/exec/pty 委托到本地 |
| 解决 | 「云端操作本地实例的专家」 | 「云端专家把工具执行放到本地机器」 |

两套系统正交，**不**用一方取代另一方。共享的是凭证原语（`infra/security/scoped_keys.py`：issue / hash / verify / revoke / rotate，含 generation 与 previous-hash 平滑轮换）与 UI 概念（设置页「远程资源」：bridge 对端与 runtime 机器并列）。

## 9. 撤销与轮换

| 操作 | 语义 |
|------|------|
| 本地删除出站连接 | 连接与反向 key 同时失效（云端下一次 `auth` 即 `reverse_auth_failed`，UI 引导清理） |
| 单独关闭反向 | 出站行 `reverse_scopes = NULL`：WS 保持（本地仍可操作云端），反向 fail-closed |
| 换 key | 本地重签发 → 重连 hello 携带新 `reverse_offer`；云端替换旧明文。旧 key 立即失效（无 previous-hash 平滑期，bridge 场景双方都在线，不需要 enrollment 那种离线轮换窗口） |
| 云端主动放弃 | 删除入站行 `reverse_key_encrypted`，仅影响云端发起能力 |

## 10. 安全考量

- 明文 key 仅经首条 hello 传输，要求 `wss://`；HTTP 明文桥上拒绝 `reverse_offer`。
- 反向执行身份是独立受限身份：无 admin、无本机用户映射、专家范围显式勾选。
- 连接级失败计数：反向鉴权连续失败按现有 WS 治理策略断连，防止 key 枚举。
- 审计：反向请求记入现有 bridge 审计日志，标注 `direction=reverse, connection_id, path`。
- 不做「云端可反向拨本地」：反向只在**已建立的** WS 上发生，本地永不监听新入站。

## 11. 分阶段落地

| 阶段 | 内容 | 验收 |
|------|------|------|
| A（最小闭环） | hello 扩展 + 两侧存储 + dialer 反向处理器 + 白名单；云端能列本地专家并开聊天 | 集成测试：双向各发起一次 agents 列表 + turn；错误路径 fail-closed |
| B | 授权 UI（本地配对时开关、专家勾选）、单独关闭反向、换 key、审计标注 | 单测覆盖撤销/轮换；dashboard 双侧入口 |
| C | `infra/security/scoped_keys.py` 抽取，runtime enrollment 迁移复用；设置页「远程资源」统一入口 | enrollment 行为不回归（现有 runtime 测试全绿） |

依赖：`api` → `infra/bridge` 边界不变；`infra/security` 为新叶子模块（仅依赖 stdlib + `infra/utils`），不反向依赖 bridge / runtime。

## 12. 开放问题

1. 反向请求是否共享正向隧道的并发/速率预算，还是单独配额（倾向：单独，避免云端反向流量挤占本地发起的对话）。
2. 云端侧多用户：一台云端多用户各自配对到同一本地实例时，反向 key 按连接行隔离即可，但本地「专家勾选」是按连接还是按本地用户统一（倾向：按连接）。
3. `hello_ack.reverse_accepted=false` 时的降级：保持单向可用（现状），UI 提示对端版本过旧。
