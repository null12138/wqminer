# WorldQuant BRAIN 社区学习总结（2026-03-08）

来源目标：`https://support.worldquantbrain.com/hc/zh-cn/community/topics`

## 1. 访问现状与结论
- 社区 topics 页当前存在 Cloudflare challenge + 登录墙。
- 直接 HTTP 访问：`403`（challenge）。
- Zendesk API 访问（`/api/v2/community/topics.json`、`/api/v2/community/posts.json`）：`Couldn't authenticate you`。
- Help Center API（`/api/v2/help_center/zh-cn/articles.json`）也返回 `401`。
- Playwright 渲染后仍落在 `Just a moment...` 挑战页，无法在无人工交互条件下稳定穿透。

结论：在当前自动化环境中，无法直接抓到社区帖子正文；可抓到的仅为登录/隐私弹窗等外围内容。

## 2. 可操作的抓取策略
为保证项目可持续使用，采用双轨方案：
- 在线抓取：支持 URL 爬取（可选 Playwright + mirror）。
- 离线抓取：支持从“已登录后导出的 HTML/Markdown/JSON/Python 文本”提取模板。

这样你只需在浏览器登录后导出页面，本项目即可批量抽取模板并进入挖掘流程。

## 3. 模板抽取规则（工程化）
抽取器采用以下规则过滤 FASTEXPR 候选：
- 语句清洗：去掉编号、列表符、反引号、尾分号。
- 赋值兼容：`alpha = expression` 自动提取右侧表达式。
- 结构校验：括号平衡。
- 语义校验：表达式里必须命中已知算子（来自 `operatorRAW.json`）。
- 去重聚合：同表达式合并多来源 URL/文件。

## 4. 本次抽取结果
本次命令：
- 种子 URL：`/hc/zh-cn/community/topics`
- 输入文件：
  - `data/cache/wqb_topics_mirror.md`（社区页镜像）
  - `~/path/to/ref-wq-brain/commands.py`
  - `~/path/to/ref-worldquant-miner/generation_one/event-based/mapc2025/templateRAW.txt`

结果：
- 页面访问：5 页（可用 4，受阻 1）
- 抽取模板：122 条
- 输出：
  - `templates/scraped_templates.json`
  - `results/community_scrape_report.json`

说明：122 条主要来自可访问参考文本；社区目标页本身因挑战/登录限制未提取到正文模板。

## 5. 对项目的优化落地
已新增能力：
- `wqminer scrape-templates` 子命令（URL + 文件双输入，支持 Playwright）。
- `wqminer community-login` 子命令（手动过 Cloudflare/登录并保存 Playwright `storage_state`）。
- 抽取结果直接生成 `templates/*.json`，可直接喂给 `wqminer mine`。

建议流程：
1. 登录社区后导出帖子页到本地文件。
2. `scrape-templates --input-file ...` 抽取模板。
3. `mine --templates-file templates/scraped_templates.json` 批量挖掘。
