# WQMiner

一个可运行的 WorldQuant 因子挖掘机，支持：
- 从 WorldQuant API 按 `region/universe/delay` 拉取对应数据字段
- 使用 LLM 基于可用算子与字段生成 FASTEXPR 因子模板
- 对模板做自动变异并提交仿真，形成挖掘闭环

## 参考实现
本项目实现思路参考了以下开源项目，并对其做了最小可用化重构：
- `zhutoutoutousan/worldquant-miner`（尤其是 `generation_two` 的字段抓取与模块化思路）
- `RussellDash332/WQ-Brain`（仿真提交流程与常用表达式实践）
 - 具体参考文件见 `REFERENCES.md`

## 对应数据字段
默认不是写死字段，而是调用 WorldQuant API：
- `GET /data-sets`（按 `category/region/universe/delay`）
- `GET /data-fields`（按数据集分页）

本地会缓存到 `data/cache/data_fields_<REGION>_<DELAY>_<UNIVERSE>.json`。
当 API 拉取失败时，才会退化到内置最小字段集（`open/high/low/close/vwap/returns/volume/adv20/cap`）。

## 安装
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## 配置
1. 复制并填写账号：`credentials.example.json` -> `credentials.json`
2. 复制并填写 LLM：`llm.example.json` -> `llm.json`

可选：使用你的自定义 OpenAI 兼容网关（支持仅填域名，会自动补全 `https://.../v1`）。

## 使用
### 1) 拉取字段
```bash
wqminer fetch-fields \
  --credentials credentials.json \
  --region USA --universe TOP3000 --delay 1
```

全量拉取（全部 data-sets + 全部 data-fields 分页）：
```bash
wqminer fetch-fields \
  --username "your_email" \
  --password "your_password" \
  --region USA --universe TOP3000 --delay 1 \
  --full \
  --output data/cache/data_fields_USA_1_TOP3000.json \
  --dataset-output data/cache/data_sets_USA_1_TOP3000.json
```

### 2) 生成模板
```bash
wqminer gen-templates \
  --llm-config llm.json \
  --credentials credentials.json \
  --region USA --universe TOP3000 --delay 1 \
  --count 24 \
  --style "prefer medium-term mean-reversion and liquidity-aware signals" \
  --output templates/generated_templates.json
```

迭代生成（含 agent 语法修复）：
```bash
wqminer gen-templates-iter \
  --llm-config llm.json \
  --fields-file data/cache/data_fields_USA_1_TOP3000_full.json \
  --region USA --universe TOP3000 --delay 1 \
  --count 60 \
  --rounds 3 \
  --max-fix-attempts 1 \
  --syntax-manual docs/fast_expr_syntax_manual.json \
  --output templates/generated_templates_iter.json \
  --report-output results/gen_templates_iter_report.json
```

可换字段模板生成（先产占位模板，再批量填充字段）：
```bash
wqminer gen-swappable \
  --llm-config /tmp/wqminer_llm_ryc.json \
  --fields-file data/cache/data_fields_USA_1_TOP3000_full.json \
  --region USA --universe TOP3000 --delay 1 \
  --template-count 120 \
  --fills-per-template 10 \
  --max-expressions 600 \
  --syntax-manual docs/fast_expr_syntax_manual.json \
  --output-swappable templates/swappable_templates.json \
  --output-filled templates/swappable_filled_templates.json \
  --report-output results/gen_swappable_report.json
```

### 3) 挖因子（真实仿真）
```bash
wqminer mine \
  --credentials credentials.json \
  --templates-file templates/generated_templates.json \
  --region USA --universe TOP3000 --delay 1 \
  --rounds 3 \
  --variants-per-template 8 \
  --max-simulations 120
```

### 4) 一键流程
```bash
wqminer run \
  --credentials credentials.json \
  --llm-config llm.json \
  --region USA --universe TOP3000 --delay 1 \
  --template-count 20 \
  --rounds 3 \
  --variants-per-template 8 \
  --max-simulations 120
```

### 5) CLI 验证（登录 + 字段 + 仿真）
默认会执行一次小规模仿真校验：
```bash
wqminer validate \
  --username "your_email" \
  --password "your_password" \
  --region USA --universe TOP3000 --delay 1
```

仅校验登录和字段，不跑仿真：
```bash
wqminer validate \
  --credentials credentials.json \
  --region USA --universe TOP3000 --delay 1 \
  --no-simulation
```

### 6) 社区模板刮削（支持 Playwright）
先手动过墙并保存登录态：
```bash
wqminer community-login \
  --start-url "https://support.worldquantbrain.com/hc/zh-cn/community/topics" \
  --state-file data/cache/community_storage_state.json \
  --wait-seconds 300
```

执行后会弹浏览器，请你手动完成 Cloudflare + 登录，命令会保存会话状态。

从社区 URL + 本地文件抽取 FASTEXPR 模板：
```bash
wqminer scrape-templates \
  --community-url "https://support.worldquantbrain.com/hc/zh-cn/community/topics" \
  --playwright \
  --playwright-state data/cache/community_storage_state.json \
  --max-pages 20 \
  --output-report results/community_scrape_report.json \
  --output-templates templates/scraped_templates.json
```

如果社区页面有登录墙/挑战页，建议把登录后页面导出到本地，再加 `--input-file`：
```bash
wqminer scrape-templates \
  --input-file data/cache/wqb_topics_after_login.html \
  --input-file data/cache/wqb_posts_after_login.html
```

刮削后直接挖掘：
```bash
wqminer mine \
  --credentials credentials.json \
  --templates-file templates/scraped_templates.json \
  --region USA --universe TOP3000 --delay 1 \
  --rounds 3 --variants-per-template 8 --max-simulations 120
```

### 7) 一次性全量采集（操作符+字段+社区模板+语法学习）
```bash
wqminer harvest-once \
  --username "your_email" \
  --password "your_password" \
  --regions "USA,GLB,EUR,CHN,ASI,IND" \
  --playwright \
  --playwright-headful \
  --playwright-state data/cache/community_storage_state.json \
  --output-dir results/harvest
```

### 8) 生成 FASTEXPR 语法手册（供 LLM 提示词）
```bash
wqminer build-syntax-manual \
  --operators-file results/harvest/operators_api_20260308_212415.json \
  --templates-file templates/scraped_templates.json \
  --output-md docs/fast_expr_syntax_manual.md \
  --output-json docs/fast_expr_syntax_manual.json
```

### 9) 并发提交回测（默认并发=3）
```bash
wqminer submit-concurrent \
  --username "your_email" \
  --password "your_password" \
  --templates-file templates/swappable_filled_templates.json \
  --region USA --universe TOP3000 --delay 1 \
  --neutralization INDUSTRY \
  --max-submissions 60 \
  --concurrency 3 \
  --max-wait 240 \
  --poll-interval 5 \
  --output-dir results/submissions
```

## 输出
- 模板文件：`templates/generated_templates_*.json`
- 挖掘日志：`results/mine_*.jsonl`
- 排行结果：`results/mine_*.csv`

## Dry-run
不连 WorldQuant，只验证“模板生成+变异+流程”是否通：
```bash
wqminer run \
  --credentials credentials.json \
  --llm-config llm.json \
  --region USA --universe TOP3000 --delay 1 \
  --dry-run
```

## 说明
- 该工具不会绕过 WorldQuant 平台规则，所有可用性取决于你的账号权限与平台返回。
- 建议先用 `--max-simulations` 小批量运行，确认质量后再放大。
