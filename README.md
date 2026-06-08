# hermes-dingtalk-approval

Hermes Agent 钉钉互动卡片审批插件。

在钉钉执行危险命令前弹出 AI Card，提供 4 个审批按钮：

- ✅ 仅此次
- ✅ 本次会话
- ✅ 永久允许
- ❌ 拒绝

## 安装

```bash
hermes plugins install file:///Users/yuyunhe/www/ai/hermes-dingtalk-approval
# 或发布到 GitHub 后：
# hermes plugins install yourusername/hermes-dingtalk-approval
```

## 前置要求

- `alibabacloud-dingtalk` SDK：`pip install alibabacloud-dingtalk`
- `dingtalk-stream` SDK：`pip install dingtalk-stream`
- 在钉钉开放平台搭建器创建并发布卡片模板，获取 template ID

## 配置

在 `~/.hermes/config.yaml` 中：

```yaml
platforms:
  dingtalk:
    enabled: true
    extra:
      approval_template_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.schema"
      # allowed_approvers: "userid1,userid2"  # 可选，留空则允许任意人点击
```

或通过环境变量（`~/.hermes/.env`）：

```
DINGTALK_APPROVAL_TEMPLATE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.schema
DINGTALK_ALLOWED_APPROVERS=userid1,userid2
```

## 卡片模板

`card_template_export.json` 是从钉钉开放平台搭建器导出的完整模板文件，可直接导入使用。

模板变量说明：

| 变量 | 说明 |
|------|------|
| `${title}` | 卡片标题 |
| `${content}` | Markdown 正文（命令预览 + 原因） |
| `${status}` | `"pending"` 显示按钮；其他值隐藏按钮 |
| `${result_label}` | 点击后显示的结果文字 |

## 工作原理

1. 插件重写 `send_exec_approval()` 方法，通过钉钉 Card API 创建并投递 AI Card
2. 同时在 Stream 连接上注册 `/v1.0/card/instances/callback` 回调 handler
3. 用户点击按钮 → 回调触发 → 调用 `resolve_gateway_approval()` → 命令执行或拒绝
4. 用 `PUT /v1.0/card/instances` + `updateCardDataByKey=True` 增量更新卡片状态

## 卡片更新注意事项

更新接口必须用 `PUT + updateCardDataByKey=True`（增量），不能用 SDK 的全量 `update_card`，
否则未传的字段（title/content）会被清空。当前实现只更新 `status` 和 `result_label`，
title 和 content 保持原样。
