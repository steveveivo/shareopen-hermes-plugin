# Changelog

## v1.2.0 (2026-11-03)
- P0 群穿透: 读 WS 帧 data.group ({members, recent, mentions, media, self_role}),
  群聊时渲染群上下文前缀 (角色/成员/最近发言/@你), 单聊零成本。
- 同步 openclaw v1.3.11 的 session-key canonical 规范对齐 (hermes 走 WS 不受影响)。

## v1.1.0
- reaction_added/removed 事件处理 (含 offline catchup)
- media 附件透传
