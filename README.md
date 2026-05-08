# WQMiner

## 简介

WQMiner 是一个 alpha 因子挖掘工具。

## 说明

本项目的 BrainAPI 相关内容因平台规则限制，不予公开。

如需获取 BrainAPI 访问权限，请按以下步骤操作：

1. 点击 GitHub 仓库右上角的 ⭐ **Star**
2. 发送邮件至 [opener@opener.eu.org](mailto:opener@opener.eu.org)，邮件标题请注明 "WQMiner BrainAPI Access Request"，并附上您的 GitHub 用户名
3. 我们将会审核您的 Star 记录，通过邮件回复您 BrainAPI 的配置信息

## 快速开始

```bash
pip install -r requirements.txt
cp run_config.example.json run_config.json
# 编辑 run_config.json，填入必要的配置信息
python3 run.py --config run_config.json
```

## 环境变量

以下环境变量需要根据您的实际配置进行设置（通过邮件申请获取）：

- `BRAIN_API_BASE_URL` — Brain API 基础地址
- `BRAIN_PLATFORM_BASE_URL` — Brain 平台基础地址

其他配置请参考 `run_config.example.json`。

## 许可

请参阅项目许可文件。
