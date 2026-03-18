#!/usr/bin/env python3
"""One-page web console: query history + control local flow."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import parse_qs, urlparse

from wqminer.config import load_run_config
from wqminer import services
from wqminer.region_config import DEFAULT_UNIVERSE, get_default_universe
from wqminer.worldquant_client import WorldQuantBrainClient


HTML_PAGE = """<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WQMiner 控制台</title>
  <style>
    :root {
      --bg: #0b0f14;
      --panel: #121826;
      --panel-2: #0e131d;
      --text: #e8ecf4;
      --muted: #9aa3b2;
      --accent: #5ad2c9;
      --accent-2: #7aa2ff;
      --danger: #ff6b6b;
      --border: #223049;
      --shadow: 0 18px 40px rgba(0,0,0,.35);
      --radius: 16px;
      font-family: "Avenir Next", "Noto Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(1200px 600px at 10% 10%, #1a2438, #0b0f14 60%),
        radial-gradient(900px 400px at 90% 0%, rgba(90,210,201,0.15), transparent 60%);
      color: var(--text);
      min-height: 100vh;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background:
        linear-gradient(120deg, rgba(255,255,255,0.04), transparent 40%),
        linear-gradient(0deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01));
      pointer-events: none;
      opacity: 0.6;
    }
    .wrap {
      max-width: 1160px;
      margin: 0 auto;
      padding: 32px 20px 48px;
      position: relative;
      z-index: 1;
    }
    h1 { margin: 0 0 8px; font-size: 26px; letter-spacing: .4px; }
    p { margin: 0; color: var(--muted); }
    .eyebrow { text-transform: uppercase; letter-spacing: 2px; font-size: 11px; color: var(--accent); font-weight: 600; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 20px;
      margin-bottom: 18px;
    }
    .hero { position: relative; overflow: hidden; }
    .hero::after {
      content: "";
      position: absolute;
      width: 240px;
      height: 240px;
      border-radius: 50%;
      right: -80px;
      top: -80px;
      background: radial-gradient(circle, rgba(122,162,255,0.18), transparent 70%);
    }
    .hero-top {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
    }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .row.tight { gap: 8px; }
    .row.wrap-top { align-items: flex-start; }
    .spacer { flex: 1 1 120px; }
    input, button, select {
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: #0c111a;
      color: var(--text);
      font-size: 14px;
    }
    input::placeholder { color: #667085; }
    input:focus, select:focus {
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(90,210,201,0.2);
    }
    button {
      background: var(--accent);
      color: #0b0f14;
      font-weight: 600;
      cursor: pointer;
      border: none;
    }
    button.secondary { background: #1a2233; color: var(--text); border: 1px solid var(--border); }
    button.danger { background: var(--danger); color: #0b0f14; }
    button.ghost { background: transparent; color: var(--muted); border: 1px dashed var(--border); }
    button:hover { filter: brightness(1.05); }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid var(--border);
      background: #0b0f14;
    }
    .status-run { color: #6be675; border-color: #1f6f3a; }
    .status-stop { color: #ff9f9f; border-color: #6f1f1f; }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .meta-card {
      background: var(--panel-2);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
    }
    .meta-label {
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 1px;
    }
    .meta-value { font-size: 14px; margin-top: 6px; }
    .muted { color: var(--muted); font-size: 12px; }
    .panel-title {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .panel-title h3 { margin: 0; font-size: 14px; letter-spacing: .3px; }
    .config-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
    }
    .config-item label {
      display: block;
      font-size: 11px;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 6px;
      letter-spacing: 1px;
    }
    .dataset-select {
      width: 100%;
      min-height: 220px;
      font-size: 12px;
      background: #0b111a;
    }
    .dataset-toolbar {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 8px;
      margin-top: 8px;
      align-items: center;
    }
    .dataset-quick-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .dataset-manual {
      width: 100%;
      min-height: 84px;
      resize: vertical;
      font-size: 12px;
      background: #0b111a;
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 8px 10px;
      box-sizing: border-box;
    }
    .dataset-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .hint-line {
      margin-top: 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .progress-grid { display: grid; gap: 10px; }
    .progress-bar {
      height: 10px;
      border-radius: 999px;
      background: #0b0f14;
      border: 1px solid var(--border);
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, rgba(90,210,201,0.9), rgba(122,162,255,0.9));
      transition: width 0.3s ease;
    }
    .progress-meta { font-size: 13px; }
    .progress-sub { font-size: 12px; color: var(--muted); }
    .output { display: grid; gap: 16px; }
    .empty {
      border: 1px dashed var(--border);
      border-radius: 12px;
      padding: 18px;
      text-align: center;
      color: var(--muted);
    }
    .result-table { width: 100%; border-collapse: collapse; }
    .result-table th {
      text-align: left;
      font-size: 12px;
      color: var(--muted);
      padding: 10px;
      border-bottom: 1px solid var(--border);
    }
    .result-table td {
      padding: 10px;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      vertical-align: top;
    }
    .result-row { border-left: 3px solid transparent; }
    .expr {
      font-family: "JetBrains Mono", "SF Mono", "Menlo", monospace;
      font-size: 12px;
      line-height: 1.4;
      word-break: break-word;
      white-space: pre-wrap;
    }
    .tag-input { min-width: 80px; max-width: 140px; }
    .color-wrap { display: flex; align-items: center; gap: 8px; }
    .color-swatch {
      width: 14px;
      height: 14px;
      border-radius: 4px;
      border: 1px solid var(--border);
      background: #101622;
      display: inline-block;
    }
    .section-header {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      margin-bottom: 8px;
    }
    .section-title { font-size: 13px; font-weight: 600; }
    .chip {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid var(--border);
      font-size: 11px;
      color: var(--muted);
      background: #0b0f14;
    }
    pre {
      white-space: pre-wrap;
      background: #0b0f14;
      border: 1px solid var(--border);
      padding: 12px;
      border-radius: 12px;
      min-height: 120px;
      max-height: 320px;
      overflow: auto;
      color: #d5d9e3;
      transition: all 0.2s ease;
    }
    pre.collapsed {
      max-height: 0;
      min-height: 0;
      padding-top: 0;
      padding-bottom: 0;
      border-color: transparent;
      overflow: hidden;
    }
    @media (max-width: 720px) {
      .panel { padding: 16px; }
      .result-table th:nth-child(2),
      .result-table td:nth-child(2),
      .result-table th:nth-child(3),
      .result-table td:nth-child(3),
      .result-table th:nth-child(4),
      .result-table td:nth-child(4) {
        display: none;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel hero">
      <div class="hero-top">
        <div>
          <div class="eyebrow">WQMiner Console</div>
          <h1>WQMiner 控制台</h1>
          <p>控制主流程 + 查询历史结果 + 标注/导出。</p>
        </div>
        <div class="actions">
          <button id="start">启动主流程</button>
          <button id="stop" class="danger">停止主流程</button>
          <span id="status" class="status-pill">状态: unknown</span>
        </div>
      </div>
      <div class="meta-grid">
        <div class="meta-card">
          <div class="meta-label">状态</div>
          <div class="meta-value" id="status-text">unknown</div>
        </div>
        <div class="meta-card">
          <div class="meta-label">结果概览</div>
          <div class="meta-value" id="meta">加载中...</div>
        </div>
        <div class="meta-card">
          <div class="meta-label">查询提示</div>
          <div class="meta-value" id="query-hint">history/top 用 limit；find 用关键词 + limit。</div>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">
        <h3>运行配置（区域/数据集）</h3>
        <span class="muted" id="config-state">读取中...</span>
      </div>
      <div class="config-grid">
        <div class="config-item">
          <label for="cfg-region">Region</label>
          <select id="cfg-region"></select>
        </div>
        <div class="config-item">
          <label for="cfg-universe">Universe</label>
          <input id="cfg-universe" placeholder="例如 TOP3000 / MINVOL1M" />
        </div>
        <div class="config-item">
          <label for="cfg-delay">Delay</label>
          <select id="cfg-delay">
            <option value="0">0</option>
            <option value="1">1</option>
          </select>
        </div>
      </div>
      <div class="dataset-actions">
        <button id="cfg-save" class="secondary">保存配置</button>
        <button id="ds-load" class="secondary">读缓存数据集</button>
        <button id="ds-refresh">刷新数据集</button>
      </div>
      <div class="hint-line" id="dataset-meta">数据集未加载</div>
      <div class="dataset-toolbar">
        <input id="ds-search" placeholder="筛选 dataset id / 分类 / 名称" />
        <div class="dataset-quick-actions">
          <button id="ds-select-all" class="secondary">全选可见</button>
          <button id="ds-clear" class="secondary">清空选择</button>
        </div>
      </div>
      <div class="row wrap-top" style="margin-top: 10px;">
        <div style="flex: 1 1 560px;">
          <select id="cfg-datasets" class="dataset-select" multiple></select>
        </div>
      </div>
      <div class="hint-line">提示：支持点选多选（无需按 Cmd/Ctrl）。启动前会自动保存当前 region/universe/delay + dataset_ids。</div>
      <div class="row wrap-top" style="margin-top: 8px;">
        <div style="flex: 1 1 560px;">
          <label for="cfg-datasets-manual" class="muted">手动 dataset_ids（兜底）</label>
          <textarea id="cfg-datasets-manual" class="dataset-manual" placeholder="接口失败或列表为空时，在这里填 dataset id（逗号/空格/换行分隔）"></textarea>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">
        <h3>进度</h3>
        <span class="muted" id="progress-state">等待启动...</span>
      </div>
      <div class="progress-grid">
        <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
        <div class="progress-meta" id="progress-meta">暂无进度</div>
        <div class="progress-sub" id="progress-sub">-</div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">
        <h3>查询与导出</h3>
        <span class="muted" id="save-state"> </span>
      </div>
      <div class="row">
        <select id="mode">
          <option value="history">最新历史</option>
          <option value="top">Sharpe 排名</option>
          <option value="find">关键词查询</option>
        </select>
        <input id="keyword" placeholder="关键词（仅 find 模式）" />
        <input id="limit" type="number" value="12" min="1" max="1000" />
        <button id="run">查询</button>
        <div class="spacer"></div>
        <select id="export-scope">
          <option value="current">导出当前</option>
          <option value="all">导出全部</option>
        </select>
        <select id="export-format">
          <option value="json">JSON</option>
          <option value="csv">CSV</option>
        </select>
        <button id="export" class="secondary">导出</button>
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">
        <h3>结果</h3>
        <span class="muted" id="result-meta">等待查询...</span>
      </div>
      <div id="output" class="output"></div>
    </div>

    <div class="panel">
      <div class="panel-title">
        <h3>日志</h3>
        <button id="toggle-log" class="ghost">展开/收起</button>
      </div>
      <pre id="log">等待日志...</pre>
    </div>
  </div>

  <script>
    const output = document.getElementById("output");
    const mode = document.getElementById("mode");
    const keyword = document.getElementById("keyword");
    const limit = document.getElementById("limit");
    const runBtn = document.getElementById("run");
    const startBtn = document.getElementById("start");
    const stopBtn = document.getElementById("stop");
    const statusEl = document.getElementById("status");
    const statusText = document.getElementById("status-text");
    const metaEl = document.getElementById("meta");
    const logEl = document.getElementById("log");
    const resultMeta = document.getElementById("result-meta");
    const saveState = document.getElementById("save-state");
    const exportBtn = document.getElementById("export");
    const exportScope = document.getElementById("export-scope");
    const exportFormat = document.getElementById("export-format");
    const logToggle = document.getElementById("toggle-log");
    const progressFill = document.getElementById("progress-fill");
    const progressMeta = document.getElementById("progress-meta");
    const progressSub = document.getElementById("progress-sub");
    const progressState = document.getElementById("progress-state");
    const cfgRegion = document.getElementById("cfg-region");
    const cfgUniverse = document.getElementById("cfg-universe");
    const cfgDelay = document.getElementById("cfg-delay");
    const cfgDatasets = document.getElementById("cfg-datasets");
    const cfgSaveBtn = document.getElementById("cfg-save");
    const dsLoadBtn = document.getElementById("ds-load");
    const dsRefreshBtn = document.getElementById("ds-refresh");
    const dsSearch = document.getElementById("ds-search");
    const dsSelectAllBtn = document.getElementById("ds-select-all");
    const dsClearBtn = document.getElementById("ds-clear");
    const cfgState = document.getElementById("config-state");
    const datasetMeta = document.getElementById("dataset-meta");
    const cfgDatasetsManual = document.getElementById("cfg-datasets-manual");

    const COLOR_OPTIONS = [
      { label: "无", value: "" },
      { label: "青绿", value: "#5ad2c9" },
      { label: "琥珀", value: "#f3b664" },
      { label: "珊瑚", value: "#ff8a7a" },
      { label: "天蓝", value: "#7aa2ff" },
      { label: "酸橙", value: "#9be15d" },
      { label: "石墨", value: "#5c677d" }
    ];

    let lastPayload = null;
    let clearTimer = null;
    let logCollapsed = false;
    let loadedDatasetOptions = [];
    let defaultUniverseMap = {};

    function formatNumber(value, digits) {
      if (typeof value !== "number" || !isFinite(value)) return "--";
      return value.toFixed(digits);
    }

    function note(msg) {
      saveState.textContent = msg;
      if (clearTimer) clearTimeout(clearTimer);
      clearTimer = setTimeout(() => {
        saveState.textContent = " ";
      }, 2500);
    }

    function setConfigState(msg) {
      cfgState.textContent = msg || "";
    }

    function splitDatasetIds(raw) {
      const source = Array.isArray(raw) ? raw.join("\n") : String(raw || "");
      const parts = source.split(/[\\s,;|]+/g);
      const out = [];
      const seen = new Set();
      parts.forEach((part) => {
        const id = String(part || "").trim();
        if (!id || seen.has(id)) return;
        seen.add(id);
        out.push(id);
      });
      return out;
    }

    function selectedDatasetIds() {
      return Array.from(cfgDatasets.selectedOptions || []).map((opt) => String(opt.value || "").trim()).filter(Boolean);
    }

    function manualDatasetIds() {
      return splitDatasetIds(cfgDatasetsManual && cfgDatasetsManual.value ? cfgDatasetsManual.value : "");
    }

    function collectDatasetIds() {
      const out = [];
      const seen = new Set();
      [...selectedDatasetIds(), ...manualDatasetIds()].forEach((id) => {
        const key = String(id || "").trim();
        if (!key || seen.has(key)) return;
        seen.add(key);
        out.push(key);
      });
      return out;
    }

    function setManualDatasetIds(ids) {
      if (!cfgDatasetsManual) return;
      cfgDatasetsManual.value = splitDatasetIds(ids).join("\n");
    }

    function setSelectedDatasetIds(ids) {
      const wanted = splitDatasetIds(ids);
      const wantedSet = new Set(wanted);
      const optionIds = new Set(Array.from(cfgDatasets.options || []).map((opt) => String(opt.value || "").trim()).filter(Boolean));
      Array.from(cfgDatasets.options || []).forEach((opt) => {
        opt.selected = wantedSet.has(String(opt.value || "").trim());
      });
      const customOnly = wanted.filter((id) => !optionIds.has(id));
      setManualDatasetIds(customOnly);
    }

    function populateRegionOptions(regions, current) {
      const values = Array.isArray(regions) && regions.length ? regions : ["USA", "GLB", "EUR", "ASI", "CHN", "IND", "JPN", "KOR"];
      cfgRegion.innerHTML = "";
      values.forEach((region) => {
        const opt = document.createElement("option");
        opt.value = String(region).toUpperCase();
        opt.textContent = String(region).toUpperCase();
        cfgRegion.appendChild(opt);
      });
      if (current) cfgRegion.value = String(current).toUpperCase();
      if (!cfgRegion.value && cfgRegion.options.length) cfgRegion.selectedIndex = 0;
    }

    function normalizeDatasetRows(datasets) {
      const out = [];
      const seen = new Set();
      (Array.isArray(datasets) ? datasets : []).forEach((raw) => {
        const id = String(raw && raw.id ? raw.id : "").trim();
        if (!id || seen.has(id)) return;
        seen.add(id);
        out.push({
          id: id,
          category: String(raw && raw.category ? raw.category : "").trim(),
          name: String(raw && raw.name ? raw.name : "").trim(),
          description: String(raw && raw.description ? raw.description : "").trim(),
          custom: false
        });
      });
      return out;
    }

    function renderDatasetOptions(datasets, selectedIds) {
      const selected = splitDatasetIds(selectedIds && selectedIds.length ? selectedIds : collectDatasetIds());
      const selectedSet = new Set(selected);
      const filter = String(dsSearch && dsSearch.value ? dsSearch.value : "").trim().toLowerCase();
      const base = normalizeDatasetRows(datasets);
      const baseSet = new Set(base.map((x) => x.id));
      loadedDatasetOptions = base;

      const rows = base.slice();
      selected.forEach((id) => {
        if (baseSet.has(id)) return;
        rows.push({
          id: id,
          category: "manual",
          name: "(自定义)",
          description: "来自已保存或手动输入的 dataset_id",
          custom: true
        });
      });

      cfgDatasets.innerHTML = "";
      let visibleCount = 0;
      rows.forEach((ds) => {
        const id = String(ds.id || "").trim();
        if (!id) return;
        const cat = String(ds.category || "").trim();
        const name = String(ds.name || "").trim();
        const desc = String(ds.description || "").trim();
        const label = [id, cat, name || desc].filter(Boolean).join(" | ");
        const searchable = `${id} ${cat} ${name} ${desc}`.toLowerCase();
        const isSelected = selectedSet.has(id);
        if (filter && !searchable.includes(filter) && !isSelected) return;
        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = ds.custom ? `${label} [manual]` : label;
        opt.title = desc || label;
        opt.selected = isSelected;
        cfgDatasets.appendChild(opt);
        visibleCount += 1;
      });

      if (visibleCount === 0) {
        const empty = document.createElement("option");
        empty.value = "";
        empty.textContent = "暂无可选数据集，可点“刷新数据集”或在下方手动输入 dataset_ids";
        empty.disabled = true;
        cfgDatasets.appendChild(empty);
      }

      const customSelected = selected.filter((id) => !baseSet.has(id));
      setManualDatasetIds(customSelected);
    }

    async function saveConfig(options = {}) {
      const silent = !!options.silent;
      const payload = {
        region: (cfgRegion.value || "").toUpperCase(),
        universe: (cfgUniverse.value || "").trim(),
        delay: parseInt(cfgDelay.value || "1", 10),
        dataset_ids: collectDatasetIds()
      };
      try {
        const resp = await fetch("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok || !data || !data.ok) {
          throw new Error((data && data.error) || "save failed");
        }
        const cfg = data.config || {};
        cfgUniverse.value = cfg.universe || cfgUniverse.value;
        cfgDelay.value = String(cfg.delay != null ? cfg.delay : cfgDelay.value || "1");
        setSelectedDatasetIds(cfg.dataset_ids || payload.dataset_ids || []);
        if (!silent) {
          setConfigState("配置已保存");
          note("配置已保存");
        }
        return cfg;
      } catch (err) {
        setConfigState("保存失败");
        if (!silent) note("配置保存失败");
        throw err;
      }
    }

    async function loadConfig() {
      try {
        const resp = await fetch("/api/config");
        const data = await resp.json();
        if (!resp.ok || !data || !data.ok) {
          throw new Error((data && data.error) || "load config failed");
        }
        const cfg = data.config || {};
        defaultUniverseMap = cfg.default_universe_map || {};
        populateRegionOptions(cfg.supported_regions || [], cfg.region || "USA");
        cfgUniverse.value = cfg.universe || "";
        cfgDelay.value = String(cfg.delay != null ? cfg.delay : 1);
        setManualDatasetIds(cfg.dataset_ids || []);
        setConfigState("配置已加载");
        const dsData = await loadDatasets(false, cfg.dataset_ids || []);
        if (!dsData || dsData.ok === false) {
          setConfigState("配置已加载（数据集未加载，已启用手动输入）");
        }
      } catch (err) {
        setConfigState("配置加载失败");
        datasetMeta.textContent = "配置加载失败";
      }
    }

    async function loadDatasets(refresh, preferredSelected) {
      const region = (cfgRegion.value || "USA").toUpperCase();
      const universe = (cfgUniverse.value || "").trim();
      const delay = parseInt(cfgDelay.value || "1", 10);
      const qs = new URLSearchParams({
        region: region,
        universe: universe,
        delay: String(delay),
        refresh: refresh ? "1" : "0"
      });
      datasetMeta.textContent = refresh ? "刷新数据集..." : "读取数据集...";
      try {
        const resp = await fetch(`/api/datasets?${qs.toString()}`);
        const data = await resp.json();
        if (!resp.ok || !data) {
          throw new Error((data && data.error) || "load datasets failed");
        }
        const datasets = data.datasets || [];
        const selected = Array.isArray(preferredSelected) ? preferredSelected : collectDatasetIds();
        renderDatasetOptions(datasets, selected);
        if (data.warning) {
          datasetMeta.textContent = `数据集 ${datasets.length} 条 | 来源 ${data.source || "-"} | ${region}/${universe || "-"} D${delay} | 警告: ${data.warning}`;
        } else {
          datasetMeta.textContent = `数据集 ${datasets.length} 条 | 来源 ${data.source || "-"} | ${region}/${universe || "-"} D${delay}`;
        }
        return data;
      } catch (err) {
        const selected = Array.isArray(preferredSelected) ? preferredSelected : collectDatasetIds();
        renderDatasetOptions([], selected);
        datasetMeta.textContent = `数据集加载失败，已切换手动输入模式: ${err}`;
        return { ok: false, error: String(err || "") };
      }
    }

    function hexToRgba(hex, alpha) {
      if (!hex || !/^#[0-9a-fA-F]{6}$/.test(hex)) return "";
      const r = parseInt(hex.slice(1, 3), 16);
      const g = parseInt(hex.slice(3, 5), 16);
      const b = parseInt(hex.slice(5, 7), 16);
      return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    function applyRowColor(row, color) {
      if (!row) return;
      row.style.borderLeftColor = color || "transparent";
      const bg = hexToRgba(color, 0.08);
      row.style.background = bg || "transparent";
    }

    function buildTable(items, title, sourceLabel) {
      const section = document.createElement("div");
      section.className = "result-section";

      const header = document.createElement("div");
      header.className = "section-header";
      const titleEl = document.createElement("div");
      titleEl.className = "section-title";
      titleEl.textContent = `${title} (${items.length})`;
      header.appendChild(titleEl);

      if (sourceLabel) {
        const chip = document.createElement("span");
        chip.className = "chip";
        chip.textContent = sourceLabel;
        header.appendChild(chip);
      }

      section.appendChild(header);

      const table = document.createElement("table");
      table.className = "result-table";

      const thead = document.createElement("thead");
      const headRow = document.createElement("tr");
      ["Expression", "Sharpe", "Fitness", "Turnover", "标签", "颜色"].forEach((label) => {
        const th = document.createElement("th");
        th.textContent = label;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      items.forEach((item) => {
        const tr = document.createElement("tr");
        tr.className = "result-row";
        applyRowColor(tr, item.color || "");

        const exprTd = document.createElement("td");
        const exprDiv = document.createElement("div");
        exprDiv.className = "expr";
        exprDiv.textContent = item.expression || "";
        exprDiv.title = item.expression || "";
        exprTd.appendChild(exprDiv);
        tr.appendChild(exprTd);

        const sharpeTd = document.createElement("td");
        sharpeTd.textContent = formatNumber(item.sharpe, 3);
        tr.appendChild(sharpeTd);

        const fitnessTd = document.createElement("td");
        fitnessTd.textContent = formatNumber(item.fitness, 3);
        tr.appendChild(fitnessTd);

        const turnoverTd = document.createElement("td");
        turnoverTd.textContent = formatNumber(item.turnover, 2);
        tr.appendChild(turnoverTd);

        const tagTd = document.createElement("td");
        const tagInput = document.createElement("input");
        tagInput.className = "tag-input";
        tagInput.placeholder = "标签";
        tagInput.value = item.tag || "";
        tagInput.addEventListener("keydown", (event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            tagInput.blur();
          }
        });
        tagInput.addEventListener("blur", () => {
          saveTag(item.expression, tagInput.value, colorSelect.value, tr);
        });
        tagTd.appendChild(tagInput);
        tr.appendChild(tagTd);

        const colorTd = document.createElement("td");
        const colorWrap = document.createElement("div");
        colorWrap.className = "color-wrap";
        const swatch = document.createElement("span");
        swatch.className = "color-swatch";
        colorWrap.appendChild(swatch);

        const colorSelect = document.createElement("select");
        colorSelect.className = "color-select";
        COLOR_OPTIONS.forEach((opt) => {
          const option = document.createElement("option");
          option.value = opt.value;
          option.textContent = opt.label;
          colorSelect.appendChild(option);
        });
        colorSelect.value = item.color || "";
        swatch.style.background = item.color || "#101622";
        colorSelect.addEventListener("change", () => {
          swatch.style.background = colorSelect.value || "#101622";
          saveTag(item.expression, tagInput.value, colorSelect.value, tr);
        });

        colorWrap.appendChild(colorSelect);
        colorTd.appendChild(colorWrap);
        tr.appendChild(colorTd);

        tbody.appendChild(tr);
      });

      table.appendChild(tbody);
      section.appendChild(table);
      return section;
    }

    function renderPayload(data) {
      output.innerHTML = "";
      const items = data.items || [];
      lastPayload = data;

      if (!items.length) {
        output.innerHTML = '<div class="empty">暂无结果</div>';
        resultMeta.textContent = "0 条结果";
        return;
      }

      if (data.mode === "find") {
        const historyItems = items.filter((item) => item.source === "history" || !item.source);
        const libraryItems = items.filter((item) => item.source === "library");

        if (historyItems.length) {
          output.appendChild(buildTable(historyItems, "历史匹配", "history"));
        }
        if (libraryItems.length) {
          output.appendChild(buildTable(libraryItems, "Library 匹配", "library"));
        }
        const counts = data.counts || {};
        const historyCount = counts.history != null ? counts.history : historyItems.length;
        const libraryCount = counts.library != null ? counts.library : libraryItems.length;
        resultMeta.textContent = `历史 ${historyCount} 条，库 ${libraryCount} 条，展示 ${items.length} 条`;
        return;
      }

      const label = data.mode === "top" ? "Sharpe 排名" : "最新历史";
      output.appendChild(buildTable(items, label));
      const counts = data.counts || {};
      const total = counts.total != null ? counts.total : items.length;
      resultMeta.textContent = `展示 ${items.length} 条 / 总计 ${total} 条`;
    }

    async function runQuery() {
      const m = mode.value;
      const n = parseInt(limit.value || "12", 10);
      const key = keyword.value.trim();
      let url = `/api/${m}?limit=${encodeURIComponent(n)}`;
      if (m === "find") {
        if (!key) {
          output.innerHTML = '<div class="empty">请输入关键词。</div>';
          return;
        }
        url += `&q=${encodeURIComponent(key)}`;
      }
      resultMeta.textContent = "查询中...";
      try {
        const resp = await fetch(url);
        const data = await resp.json();
        renderPayload(data);
      } catch (err) {
        output.innerHTML = `<div class="empty">查询失败: ${err}</div>`;
      }
    }

    async function saveTag(expression, tag, color, rowEl) {
      try {
        const payload = { expression, tag: (tag || "").trim(), color: color || "" };
        const resp = await fetch("/api/tag", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (!resp.ok) {
          note("标注保存失败");
          return;
        }
        const saved = data && data.item ? data.item : payload;
        applyRowColor(rowEl, saved.color || "");
        updateLocalTags(expression, saved.tag || "", saved.color || "");
        note("标注已保存");
      } catch (err) {
        note("标注保存失败");
      }
    }

    function updateLocalTags(expression, tag, color) {
      if (!lastPayload || !Array.isArray(lastPayload.items)) return;
      lastPayload.items.forEach((item) => {
        if (item.expression === expression) {
          item.tag = tag;
          item.color = color;
        }
      });
    }

    async function fetchProgress() {
      try {
        const resp = await fetch("/api/progress");
        const data = await resp.json();
        const running = data.running;
        statusEl.textContent = running ? "状态: 运行中" : "状态: 已停止";
        statusEl.className = running ? "status-pill status-run" : "status-pill status-stop";
        statusText.textContent = running ? "运行中" : "已停止";
        const results = data.results || {};
        const count = results.count || "0";
        const latest = results.latest || "-";
        metaEl.textContent = `results=${count} | latest=${latest}`;

        const total = Number(data.total || 0);
        const completed = Number(data.completed || 0);
        const success = Number(data.success || 0);
        const failed = Number(data.failed || 0);
        const round = Number(data.round || 0);
        const stage = data.stage || "-";
        const pct = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 0;
        progressFill.style.width = `${pct}%`;
        progressState.textContent = running ? `运行中 · ${stage}` : "已停止";
        progressMeta.textContent = total > 0 ? `完成 ${completed}/${total} (${pct}%)` : "等待任务...";
        const last = data.last || {};
        const lastExpr = (last.expression || "").toString().trim();
        const lastInfo = lastExpr ? `最近: ${lastExpr.slice(0, 80)}` : "暂无最近因子";
        progressSub.textContent = `成功 ${success} · 失败 ${failed} · Round ${round} · ${lastInfo}`;
      } catch (err) {
        statusEl.textContent = "状态: unknown";
        statusText.textContent = "unknown";
        metaEl.textContent = "状态获取失败: " + err;
      }
    }

    async function fetchLog() {
      if (logCollapsed) return;
      try {
        const resp = await fetch("/api/status");
        const data = await resp.json();
        logEl.textContent = data.log || "暂无日志";
      } catch (err) {
        logEl.textContent = "日志获取失败: " + err;
      }
    }

    function exportData() {
      const m = mode.value;
      const key = keyword.value.trim();
      const n = parseInt(limit.value || "12", 10);
      if (m === "find" && !key) {
        note("请先输入关键词");
        return;
      }
      const scope = exportScope.value;
      const format = exportFormat.value;
      const limitValue = scope === "all" ? 0 : n;
      const params = new URLSearchParams({
        mode: m,
        limit: String(limitValue),
        format: format
      });
      if (m === "find") {
        params.set("q", key);
      }
      window.location.href = `/api/export?${params.toString()}`;
    }

    startBtn.addEventListener("click", async () => {
      try {
        await saveConfig({ silent: true });
      } catch (err) {
        note("启动前保存配置失败");
        return;
      }
      await fetch("/api/start", { method: "POST" });
      fetchProgress();
      fetchLog();
    });
    stopBtn.addEventListener("click", async () => {
      await fetch("/api/stop", { method: "POST" });
      fetchProgress();
      fetchLog();
    });
    runBtn.addEventListener("click", runQuery);
    exportBtn.addEventListener("click", exportData);
    cfgSaveBtn.addEventListener("click", async () => {
      try {
        await saveConfig();
      } catch (err) {}
    });
    dsLoadBtn.addEventListener("click", async () => {
      try {
        await loadDatasets(false);
      } catch (err) {}
    });
    dsRefreshBtn.addEventListener("click", async () => {
      try {
        await loadDatasets(true);
      } catch (err) {}
    });
    cfgDatasets.addEventListener("mousedown", (event) => {
      const target = event.target;
      if (!target || target.tagName !== "OPTION" || target.disabled) return;
      event.preventDefault();
      target.selected = !target.selected;
    });
    dsSearch.addEventListener("input", () => {
      renderDatasetOptions(loadedDatasetOptions, collectDatasetIds());
    });
    dsSelectAllBtn.addEventListener("click", () => {
      Array.from(cfgDatasets.options || []).forEach((opt) => {
        if (opt.disabled || !opt.value) return;
        opt.selected = true;
      });
    });
    dsClearBtn.addEventListener("click", () => {
      Array.from(cfgDatasets.options || []).forEach((opt) => {
        opt.selected = false;
      });
      setManualDatasetIds([]);
      note("已清空 dataset 选择");
    });
    cfgDatasetsManual.addEventListener("blur", () => {
      const manual = manualDatasetIds();
      const selected = selectedDatasetIds();
      renderDatasetOptions(loadedDatasetOptions, [...selected, ...manual]);
    });
    cfgRegion.addEventListener("change", async () => {
      if (!cfgUniverse.value.trim()) {
        const fallback = defaultUniverseMap[cfgRegion.value] || "";
        if (fallback) cfgUniverse.value = fallback;
      }
      try {
        await loadDatasets(false);
      } catch (err) {}
    });
    cfgDelay.addEventListener("change", async () => {
      try {
        await loadDatasets(false);
      } catch (err) {}
    });
    cfgUniverse.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadDatasets(false).catch(() => {});
      }
    });
    logToggle.addEventListener("click", () => {
      logEl.classList.toggle("collapsed");
      logCollapsed = logEl.classList.contains("collapsed");
      if (!logCollapsed) {
        fetchLog();
      }
    });

    loadConfig();
    fetchProgress();
    fetchLog();
    setInterval(fetchProgress, 2000);
    setInterval(fetchLog, 6000);
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WQMiner web console")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8002, help="Port to bind")
    parser.add_argument("--config", default="run_config.json", help="Run config JSON path")
    parser.add_argument("--results-dir", default="", help="Override results dir for query")
    parser.add_argument("--library", default="", help="Override library path for query")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--log-limit", type=int, default=200, help="Log lines kept in memory")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def _get(cfg: dict, key: str, default):
    value = cfg.get(key, default)
    return default if value is None else value


def _load_results(results_dir: str) -> List[Dict[str, float]]:
    root = Path(results_dir)
    if not root.exists():
        return []
    records: List[Dict[str, float]] = []
    files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime)
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        ts = path.stat().st_mtime
        for idx, row in enumerate(payload):
            if not isinstance(row, dict):
                continue
            expr = str(row.get("expression", "")).strip()
            if not expr:
                continue
            try:
                sharpe = float(row.get("sharpe", 0.0))
                fitness = float(row.get("fitness", 0.0))
                turnover = float(row.get("turnover", 0.0))
            except Exception:
                sharpe = 0.0
                fitness = 0.0
                turnover = 0.0
            records.append(
                {
                    "expression": expr,
                    "sharpe": sharpe,
                    "fitness": fitness,
                    "turnover": turnover,
                    "_ts": ts,
                    "_idx": idx,
                }
            )
    records.sort(key=lambda r: (r.get("_ts", 0.0), r.get("_idx", 0)))
    return records


def _load_library(path: str) -> List[str]:
    src = Path(path)
    if not src.exists():
        return []
    try:
        payload = json.loads(src.read_text(encoding="utf-8"))
    except Exception:
        return []
    expressions: List[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("expression"):
                expressions.append(str(item.get("expression", "")).strip())
            elif isinstance(item, str):
                expressions.append(item.strip())
    elif isinstance(payload, dict) and isinstance(payload.get("templates"), list):
        for item in payload.get("templates", []):
            if isinstance(item, dict) and item.get("expression"):
                expressions.append(str(item.get("expression", "")).strip())
    return [x for x in expressions if x]


def _format_rows(rows: List[Dict[str, float]], limit: int) -> str:
    if not rows:
        return "none"
    lines = []
    view = rows if int(limit) <= 0 else rows[: max(1, int(limit))]
    for row in view:
        lines.append(
            f"{row.get('expression','')} | sharpe={row.get('sharpe',0.0):.3f} "
            f"fitness={row.get('fitness',0.0):.3f} turnover={row.get('turnover',0.0):.2f}"
        )
    return "\n".join(lines)


def _format_library(rows: List[str], limit: int) -> str:
    if not rows:
        return "none"
    view = rows if int(limit) <= 0 else rows[: max(1, int(limit))]
    return "\n".join(view)


def _results_stats(results_dir: str) -> Dict[str, str]:
    root = Path(results_dir)
    if not root.exists():
        return {"count": "0", "latest": "none"}
    files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return {"count": "0", "latest": "none"}
    return {"count": str(len(files)), "latest": files[-1].name}


TAG_MAX_LEN = 24
_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")


def _normalize_tag(value: str) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    return text[:TAG_MAX_LEN]


def _normalize_color(value: str) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    return text.lower() if _COLOR_RE.match(text) else ""


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


SUPPORTED_REGIONS: List[str] = sorted(
    {
        "USA",
        "GLB",
        "EUR",
        "ASI",
        "CHN",
        "IND",
        "JPN",
        "KOR",
        "TWN",
        "HKG",
        *DEFAULT_UNIVERSE.keys(),
    }
)
DEFAULT_DATASET_CATEGORIES: Sequence[str] = ("fundamental", "analyst", "model", "news", "alternative")


def _safe_slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_-]+", "", str(value or "").strip())
    return text or "default"


def _normalize_dataset_ids(value: Any) -> List[str]:
    out: List[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        parts = [x.strip() for x in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = [str(x).strip() for x in value]
    else:
        parts = [str(value).strip()]
    seen = set()
    for item in parts:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _dataset_cache_path(region: str, universe: str, delay: int) -> Path:
    r = _safe_slug(str(region or "USA").upper())
    u = _safe_slug(str(universe or "DEFAULT").upper())
    d = max(0, int(delay))
    return Path("data/cache") / f"datasets_{r}_{d}_{u}.json"


def _normalize_dataset_row(raw: Dict[str, Any]) -> Optional[Dict[str, str]]:
    ds_id = str(raw.get("id", "") or "").strip()
    if not ds_id:
        return None
    category = ""
    cat_raw = raw.get("category")
    if isinstance(cat_raw, dict):
        category = str(cat_raw.get("id", "") or cat_raw.get("name", "") or "").strip()
    else:
        category = str(raw.get("category_id", "") or raw.get("category", "") or "").strip()
    name = str(raw.get("name", "") or "").strip()
    description = str(raw.get("description", "") or "").strip()
    return {
        "id": ds_id,
        "name": name,
        "description": description,
        "category": category,
    }


def _load_dataset_cache(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        items = payload.get("datasets", [])
    elif isinstance(payload, list):
        items = payload
    else:
        return []
    out: List[Dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_dataset_row(item)
        if normalized is None:
            continue
        out.append(normalized)
    out.sort(key=lambda x: (x.get("category", ""), x.get("id", "")))
    return out


def _save_dataset_cache(path: Path, *, region: str, universe: str, delay: int, datasets: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "region": str(region or "").upper(),
        "universe": str(universe or ""),
        "delay": int(delay),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "datasets": list(datasets),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


class TagStore:
    def __init__(self, path: Path):
        self._path = Path(path)
        self._lock = threading.Lock()

    def set_path(self, path: Path) -> None:
        with self._lock:
            self._path = Path(path)

    def load_all(self) -> Dict[str, Dict[str, str]]:
        with self._lock:
            return self._read()

    def set(self, expression: str, tag: str, color: str) -> Dict[str, str]:
        expr = str(expression or "").strip()
        tag = _normalize_tag(tag)
        color = _normalize_color(color)
        with self._lock:
            data = self._read()
            if not expr:
                return {"expression": "", "tag": "", "color": ""}
            if tag or color:
                data[expr] = {"tag": tag, "color": color}
            else:
                data.pop(expr, None)
            self._write(data)
        return {"expression": expr, "tag": tag, "color": color}

    def _read(self) -> Dict[str, Dict[str, str]]:
        if not self._path.exists():
            return {}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        cleaned: Dict[str, Dict[str, str]] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            tag = _normalize_tag(value.get("tag", ""))
            color = _normalize_color(value.get("color", ""))
            if tag or color:
                cleaned[key] = {"tag": tag, "color": color}
        return cleaned

    def _write(self, data: Dict[str, Dict[str, str]]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)


def _attach_tags(rows: List[Dict[str, float]], tags: Dict[str, Dict[str, str]], source: str) -> None:
    for row in rows:
        expr = str(row.get("expression", "")).strip()
        info = tags.get(expr, {})
        row["tag"] = info.get("tag", "")
        row["color"] = info.get("color", "")
        row["source"] = source


def _library_items(expressions: List[str], tags: Dict[str, Dict[str, str]]) -> List[Dict[str, Optional[float]]]:
    items: List[Dict[str, Optional[float]]] = []
    for expr in expressions:
        expr = str(expr or "").strip()
        if not expr:
            continue
        info = tags.get(expr, {})
        items.append(
            {
                "expression": expr,
                "sharpe": None,
                "fitness": None,
                "turnover": None,
                "tag": info.get("tag", ""),
                "color": info.get("color", ""),
                "source": "library",
            }
        )
    return items


def _query_payload(state: "FlowController", mode: str, limit: int, key: str) -> Dict:
    records = _load_results(state.results_dir)
    tags = state.tag_store.load_all() if state.tag_store else {}
    mode = mode or "history"

    if mode == "history":
        rows = records if limit <= 0 else records[-limit:]
        _attach_tags(rows, tags, "history")
        return {
            "mode": "history",
            "items": rows,
            "counts": {"total": len(records)},
            "text": _format_rows(rows, limit),
        }

    if mode == "top":
        ranked = sorted(records, key=lambda r: r.get("sharpe", 0.0), reverse=True)
        rows = ranked if limit <= 0 else ranked[:limit]
        _attach_tags(rows, tags, "history")
        return {
            "mode": "top",
            "items": rows,
            "counts": {"total": len(records)},
            "text": _format_rows(rows, limit),
        }

    if mode == "find":
        key = (key or "").strip().lower()
        if not key:
            return {"error": "missing keyword"}
        matches = [r for r in records if key in r.get("expression", "").lower()]
        view = matches[::-1]
        view = view if limit <= 0 else view[:limit]
        _attach_tags(view, tags, "history")
        library = _load_library(state.library_path)
        lib_matches = [x for x in library if key in x.lower()]
        lib_view = lib_matches if limit <= 0 else lib_matches[:limit]
        lib_items = _library_items(lib_view, tags)
        text = "\n".join(
            [
                f"History matches: {len(matches)}",
                _format_rows(view, limit),
                f"Library matches: {len(lib_matches)}",
                _format_library(lib_view, limit),
            ]
        )
        return {
            "mode": "find",
            "items": view + lib_items,
            "counts": {"history": len(matches), "library": len(lib_matches)},
            "text": text,
        }

    return {"error": "unsupported mode"}


def _items_to_csv(items: List[Dict]) -> bytes:
    fields = ["expression", "sharpe", "fitness", "turnover", "tag", "color", "source", "_ts", "_idx"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for item in items:
        writer.writerow({field: item.get(field, "") for field in fields})
    return buffer.getvalue().encode("utf-8")


class LogBufferHandler(logging.Handler):
    def __init__(self, limit: int = 200):
        super().__init__()
        self.limit = max(50, int(limit))
        self._lines: List[str] = []
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        with self._lock:
            self._lines.append(msg)
            if len(self._lines) > self.limit:
                self._lines = self._lines[-self.limit :]

    def get_text(self) -> str:
        with self._lock:
            return "\n".join(self._lines[-self.limit :])


class FlowController:
    def __init__(self, config_path: str, results_dir_override: str, library_override: str):
        self.config_path = config_path
        self.results_dir_override = results_dir_override
        self.library_override = library_override
        self.lock = threading.Lock()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.stop_event: Optional[threading.Event] = None
        self.last_start = ""
        self.last_stop = ""
        self.last_error = ""
        self.last_summary: Optional[Dict] = None
        self.results_dir = results_dir_override or "results/one_click"
        self.library_path = library_override or "templates/library.json"
        self.tag_store = TagStore(Path(self.results_dir) / "tags.json")
        self.progress_lock = threading.Lock()
        self.progress = {
            "stage": "idle",
            "round": 0,
            "total": 0,
            "completed": 0,
            "success": 0,
            "failed": 0,
            "last": {},
            "updated": self._now(),
        }

    def _read_config(self) -> Dict[str, Any]:
        try:
            cfg = load_run_config(self.config_path)
        except Exception:
            cfg = {}
        if not isinstance(cfg, dict):
            cfg = {}
        return cfg

    def _write_config(self, cfg: Dict[str, Any]) -> None:
        target = Path(self.config_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    def _apply_paths_from_config(self, cfg: Dict[str, Any]) -> None:
        output_dir = self.results_dir_override or _get(cfg, "output_dir", "results/one_click")
        library_output = self.library_override or _get(cfg, "library_output", "templates/library.json")
        self.results_dir = output_dir
        self.library_path = library_output
        self.tag_store.set_path(Path(self.results_dir) / "tags.json")

    def get_config_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            cfg = self._read_config()
            self._apply_paths_from_config(cfg)
        region = str(_get(cfg, "region", "USA")).upper()
        universe = str(_get(cfg, "universe", get_default_universe(region)))
        delay = int(_get(cfg, "delay", 1))
        dataset_ids = _normalize_dataset_ids(_get(cfg, "dataset_ids", []))
        return {
            "region": region,
            "universe": universe,
            "delay": delay,
            "dataset_ids": dataset_ids,
            "config_path": self.config_path,
            "results_dir": self.results_dir,
            "library_path": self.library_path,
            "running": self.running,
            "supported_regions": SUPPORTED_REGIONS,
            "default_universe_map": dict(DEFAULT_UNIVERSE),
        }

    def update_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("payload must be object")
        with self.lock:
            cfg = self._read_config()
            region = str(payload.get("region", _get(cfg, "region", "USA"))).strip().upper() or "USA"
            universe = str(payload.get("universe", _get(cfg, "universe", get_default_universe(region)))).strip()
            if not universe:
                universe = get_default_universe(region)
            delay = _parse_int(str(payload.get("delay", _get(cfg, "delay", 1))), int(_get(cfg, "delay", 1)))
            delay = max(0, delay)
            dataset_ids = _normalize_dataset_ids(payload.get("dataset_ids", _get(cfg, "dataset_ids", [])))

            cfg["region"] = region
            cfg["universe"] = universe
            cfg["delay"] = delay
            cfg["dataset_ids"] = dataset_ids

            self._write_config(cfg)
            self._apply_paths_from_config(cfg)

        return {
            "region": region,
            "universe": universe,
            "delay": delay,
            "dataset_ids": dataset_ids,
            "supported_regions": SUPPORTED_REGIONS,
            "default_universe_map": dict(DEFAULT_UNIVERSE),
        }

    def list_datasets(
        self,
        *,
        region: str,
        universe: str,
        delay: int,
        refresh: bool = False,
    ) -> Dict[str, Any]:
        region = str(region or "USA").upper()
        universe = str(universe or get_default_universe(region))
        delay = max(0, int(delay))
        cache_path = _dataset_cache_path(region, universe, delay)

        if not refresh:
            cached = _load_dataset_cache(cache_path)
            if cached:
                return {
                    "region": region,
                    "universe": universe,
                    "delay": delay,
                    "source": "cache",
                    "cache_file": str(cache_path),
                    "datasets": cached,
                }

        cfg = self._read_config()
        user, pwd = services.resolve_credentials(
            credentials_path=str(_get(cfg, "credentials", "")),
            username=str(_get(cfg, "username", "")),
            password=str(_get(cfg, "password", "")),
            required=True,
        )
        client = WorldQuantBrainClient(
            username=user,
            password=pwd,
            timeout_sec=max(5, int(_get(cfg, "timeout_sec", 60))),
            max_retries=max(1, int(_get(cfg, "max_retries", 5))),
            disable_proxy=bool(_get(cfg, "disable_proxy", False)),
        )
        raw_categories = _get(cfg, "dataset_categories", list(DEFAULT_DATASET_CATEGORIES))
        if isinstance(raw_categories, str):
            categories = [x.strip() for x in raw_categories.split(",") if x.strip()]
        elif isinstance(raw_categories, (list, tuple)):
            categories = [str(x).strip() for x in raw_categories if str(x).strip()]
        else:
            categories = list(DEFAULT_DATASET_CATEGORIES)
        if not categories:
            categories = list(DEFAULT_DATASET_CATEGORIES)
        page_limit = max(1, min(100, int(_get(cfg, "dataset_page_limit", 50))))
        max_pages = int(_get(cfg, "dataset_max_pages", 2))

        datasets = client.fetch_all_datasets(
            region=region,
            universe=universe,
            delay=delay,
            categories=categories,
            dataset_page_limit=page_limit,
            dataset_max_pages=max_pages,
        )
        normalized: List[Dict[str, str]] = []
        for raw in datasets:
            normalized_row = _normalize_dataset_row(raw)
            if normalized_row is None:
                continue
            normalized.append(normalized_row)
        normalized.sort(key=lambda x: (x.get("category", ""), x.get("id", "")))
        _save_dataset_cache(cache_path, region=region, universe=universe, delay=delay, datasets=normalized)
        return {
            "region": region,
            "universe": universe,
            "delay": delay,
            "source": "live",
            "cache_file": str(cache_path),
            "datasets": normalized,
        }

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def update_progress(self, **fields) -> None:
        with self.progress_lock:
            self.progress.update(fields)
            self.progress["updated"] = self._now()

    def get_progress(self) -> Dict:
        with self.progress_lock:
            return dict(self.progress)

    def start(self) -> str:
        with self.lock:
            if self.running:
                return "already running"
            self.stop_event = threading.Event()
            self.running = True
            self.last_error = ""
            self.last_start = self._now()
            self.update_progress(
                stage="starting",
                round=0,
                total=0,
                completed=0,
                success=0,
                failed=0,
                last={},
            )
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            return "started"

    def stop(self) -> str:
        with self.lock:
            if not self.running or not self.stop_event:
                return "not running"
            self.stop_event.set()
            return "stop requested"

    def _run(self) -> None:
        try:
            cfg = self._read_config()
            self._apply_paths_from_config(cfg)
            output_dir = self.results_dir
            library_output = self.library_path
            self.update_progress(stage="setup")

            def progress_cb(payload: Dict) -> None:
                if not payload:
                    return
                self.update_progress(**payload)

            summary = services.run_one_click(
                region=_get(cfg, "region", "USA"),
                universe=_get(cfg, "universe", ""),
                delay=int(_get(cfg, "delay", 1)),
                llm_config_path=_get(cfg, "llm_config", "llm.json"),
                credentials_path=_get(cfg, "credentials", ""),
                username=_get(cfg, "username", ""),
                password=_get(cfg, "password", ""),
                template_count=int(_get(cfg, "template_count", 20)),
                style_prompt=_get(cfg, "style", ""),
                inspiration=_get(cfg, "inspiration", ""),
                output_dir=output_dir,
                concurrency=int(_get(cfg, "concurrency", 3)),
                async_mode=bool(_get(cfg, "async_mode", False)),
                timeout_sec=int(_get(cfg, "timeout_sec", 60)),
                max_retries=int(_get(cfg, "max_retries", 5)),
                poll_interval_sec=int(_get(cfg, "poll_interval", 30)),
                max_wait_sec=int(_get(cfg, "max_wait", 600)),
                max_rounds=int(_get(cfg, "max_rounds", 0)),
                sleep_between_rounds=int(_get(cfg, "sleep_between_rounds", 5)),
                evolve_rounds=int(_get(cfg, "evolve_rounds", 0)),
                evolve_count=int(_get(cfg, "evolve_count", 0)),
                evolve_top_k=int(_get(cfg, "evolve_top_k", 6)),
                concurrency_cap=int(_get(cfg, "concurrency_cap", 0)),
                seed_templates=_get(cfg, "seed_templates", ""),
                library_output=library_output,
                library_sharpe_min=float(_get(cfg, "library_sharpe_min", 1.2)),
                library_fitness_min=float(_get(cfg, "library_fitness_min", 1.0)),
                reverse_sharpe_max=float(_get(cfg, "reverse_sharpe_max", -1.2)),
                reverse_fitness_max=float(_get(cfg, "reverse_fitness_max", -1.0)),
                reverse_log=_get(cfg, "reverse_log", ""),
                negate_max_per_round=int(_get(cfg, "negate_max_per_round", 0)),
                retry_failed_rounds=int(_get(cfg, "retry_failed_rounds", 2)),
                retry_failed_sleep=int(_get(cfg, "retry_failed_sleep", 2)),
                disable_proxy=bool(_get(cfg, "disable_proxy", False)),
                notify_url=_get(cfg, "notify_url", ""),
                operator_file=_get(cfg, "operator_file", ""),
                strict_validation=bool(_get(cfg, "strict_validation", False)),
                max_operator_count=int(_get(cfg, "max_operator_count", 0)),
                require_keyword_optional=bool(_get(cfg, "require_keyword_optional", True)),
                batch_size=int(_get(cfg, "batch_size", 0)),
                enforce_exact_batch=bool(_get(cfg, "enforce_exact_batch", False)),
                required_theme_coverage=int(_get(cfg, "required_theme_coverage", 0)),
                common_operator_limit=int(_get(cfg, "common_operator_limit", 0)),
                enforce_explore_theme_pairs=bool(_get(cfg, "enforce_explore_theme_pairs", False)),
                template_guide_path=_get(cfg, "template_guide_path", ""),
                template_style_items=int(_get(cfg, "template_style_items", 0)),
                template_seed_count=int(_get(cfg, "template_seed_count", 0)),
                dataset_ids=_get(cfg, "dataset_ids", []),
                dataset_field_max_pages=int(_get(cfg, "dataset_field_max_pages", 5)),
                dataset_field_page_limit=int(_get(cfg, "dataset_field_page_limit", 50)),
                results_append_file=_get(cfg, "results_append_file", ""),
                baseline_alpha_id=_get(cfg, "baseline_alpha_id", ""),
                progress_cb=progress_cb,
                stop_event=self.stop_event,
            )
            self.last_summary = summary
        except Exception as exc:
            self.last_error = str(exc)
            logging.exception("Flow failed: %s", exc)
        finally:
            with self.lock:
                self.running = False
                self.last_stop = self._now()
            self.update_progress(stage="idle")


APP_STATE: Optional[FlowController] = None
LOG_BUFFER: Optional[LogBufferHandler] = None


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Dict, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_bytes(
        self,
        data: bytes,
        content_type: str,
        status: int = 200,
        filename: Optional[str] = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/config":
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            self._send_json({"ok": True, "config": state.get_config_snapshot()})
            return

        if parsed.path == "/api/datasets":
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            qs = parse_qs(parsed.query)
            snapshot = state.get_config_snapshot()
            region = str((qs.get("region", [snapshot.get("region", "USA")])[0] or snapshot.get("region", "USA"))).strip()
            universe = str((qs.get("universe", [snapshot.get("universe", "")])[0] or snapshot.get("universe", ""))).strip()
            delay = _parse_int(str(qs.get("delay", [snapshot.get("delay", 1)])[0] or snapshot.get("delay", 1)), int(snapshot.get("delay", 1)))
            refresh_raw = str((qs.get("refresh", ["0"])[0] or "0")).strip().lower()
            refresh = refresh_raw in {"1", "true", "yes", "y", "on"}
            try:
                payload = state.list_datasets(region=region, universe=universe, delay=delay, refresh=refresh)
            except Exception as exc:
                fallback_ids = _normalize_dataset_ids(snapshot.get("dataset_ids", []))
                fallback_rows = [
                    {
                        "id": ds_id,
                        "name": "",
                        "description": "from config.dataset_ids",
                        "category": "manual",
                    }
                    for ds_id in fallback_ids
                ]
                self._send_json(
                    {
                        "ok": True,
                        "region": str(region or "USA").upper(),
                        "universe": str(universe or ""),
                        "delay": max(0, int(delay)),
                        "source": "fallback",
                        "warning": str(exc),
                        "datasets": fallback_rows,
                    }
                )
                return
            self._send_json({"ok": True, **payload})
            return

        if parsed.path == "/api/status":
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            stats = _results_stats(state.results_dir)
            meta = (
                f"running={state.running} | results={stats['count']} | latest={stats['latest']} | "
                f"start={state.last_start or '-'} | stop={state.last_stop or '-'} | "
                f"error={state.last_error or '-'}"
            )
            self._send_json(
                {
                    "running": state.running,
                    "meta": meta,
                    "log": LOG_BUFFER.get_text() if LOG_BUFFER else "",
                }
            )
            return

        if parsed.path == "/api/progress":
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            stats = _results_stats(state.results_dir)
            progress = state.get_progress()
            progress.update(
                {
                    "running": state.running,
                    "results": stats,
                    "last_error": state.last_error or "",
                    "last_start": state.last_start or "",
                    "last_stop": state.last_stop or "",
                }
            )
            self._send_json(progress)
            return

        if parsed.path.startswith("/api/"):
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            qs = parse_qs(parsed.query)
            limit = _parse_int(qs.get("limit", ["12"])[0] or "12", 12)

            if parsed.path == "/api/export":
                mode = (qs.get("mode", ["history"])[0] or "history").strip()
                fmt = (qs.get("format", ["json"])[0] or "json").strip().lower()
                key = (qs.get("q", [""])[0] or "").strip()
                payload = _query_payload(state, mode, limit, key)
                if payload.get("error"):
                    self._send_json(payload, status=400)
                    return
                ts = time.strftime("%Y%m%d_%H%M%S")
                if fmt == "csv":
                    data = _items_to_csv(payload.get("items", []))
                    filename = f"wqminer_{mode}_{ts}.csv"
                    self._send_bytes(data, "text/csv; charset=utf-8", filename=filename)
                    return
                data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                filename = f"wqminer_{mode}_{ts}.json"
                self._send_bytes(data, "application/json; charset=utf-8", filename=filename)
                return

            if parsed.path in {"/api/history", "/api/top", "/api/find"}:
                mode = parsed.path.replace("/api/", "", 1)
                key = (qs.get("q", [""])[0] or "").strip()
                payload = _query_payload(state, mode, limit, key)
                if payload.get("error"):
                    self._send_json(payload, status=400)
                    return
                self._send_json(payload)
                return

        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        state = APP_STATE
        if not state:
            self._send_json({"error": "state not ready"}, status=500)
            return
        if parsed.path == "/api/config":
            length = _parse_int(self.headers.get("Content-Length", "0"), 0)
            if length <= 0:
                self._send_json({"error": "missing body"}, status=400)
                return
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception:
                self._send_json({"error": "invalid json"}, status=400)
                return
            try:
                config = state.update_config(payload if isinstance(payload, dict) else {})
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            self._send_json({"ok": True, "config": config})
            return
        if parsed.path == "/api/start":
            msg = state.start()
            self._send_json({"ok": True, "message": msg})
            return
        if parsed.path == "/api/stop":
            msg = state.stop()
            self._send_json({"ok": True, "message": msg})
            return
        if parsed.path == "/api/tag":
            length = _parse_int(self.headers.get("Content-Length", "0"), 0)
            if length <= 0:
                self._send_json({"error": "missing body"}, status=400)
                return
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception:
                self._send_json({"error": "invalid json"}, status=400)
                return
            expr = str(payload.get("expression", "") or "").strip()
            if not expr:
                self._send_json({"error": "missing expression"}, status=400)
                return
            tag = payload.get("tag", "")
            color = payload.get("color", "")
            updated = state.tag_store.set(expr, tag, color)
            self._send_json({"ok": True, "item": updated})
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt: str, *args) -> None:
        msg = fmt % args
        if "/api/status" in msg or "/api/progress" in msg or "/api/config" in msg or "/api/datasets" in msg:
            return
        logging.info("%s - %s", self.address_string(), msg)


def main() -> int:
    global APP_STATE, LOG_BUFFER
    args = parse_args()
    configure_logging(args.log_level)

    LOG_BUFFER = LogBufferHandler(limit=args.log_limit)
    LOG_BUFFER.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(LOG_BUFFER)

    APP_STATE = FlowController(args.config, args.results_dir, args.library)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    logging.info("Web console listening on http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
