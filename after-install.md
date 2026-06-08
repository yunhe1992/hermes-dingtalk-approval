# 安装后配置说明

## 1. 安装钉钉 SDK 依赖

```bash
pip install alibabacloud-dingtalk dingtalk-stream
```

## 2. 导入卡片模板

将 `card_template_export.json` 导入钉钉开放平台搭建器，发布后获取模板 ID（格式：`xxxxxxxx.schema`）。

## 3. 在 config.yaml 中配置模板 ID

```yaml
platforms:
  dingtalk:
    enabled: true
    extra:
      approval_template_id: "你的模板ID.schema"
```

## 4. 重启 gateway

```bash
hermes gateway restart
```

完成后，在钉钉触发危险命令时将自动弹出审批卡片。
