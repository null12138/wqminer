(function () {
  function q(id) {
    return document.getElementById(id);
  }

  function formToObject(form) {
    var obj = {};
    var elements = form.querySelectorAll("input, textarea, select");
    elements.forEach(function (el) {
      if (!el.name) {
        return;
      }
      if (el.type === "checkbox") {
        obj[el.name] = el.checked;
        return;
      }
      obj[el.name] = el.value;
    });
    return obj;
  }

  function setText(el, text) {
    if (!el) {
      return;
    }
    el.textContent = text;
  }

  async function submitJob(form) {
    if (form.dataset.submitting === "1") {
      return;
    }
    form.dataset.submitting = "1";
    var btn = form.querySelector("button[type='submit']");
    if (btn) {
      btn.disabled = true;
    }

    var statusEl = q(form.dataset.statusTarget || "job-status");
    var resultEl = q(form.dataset.resultTarget || "job-result");

    setText(statusEl, "Submitting job...");
    setText(resultEl, "");

    var payload = {
      type: form.dataset.jobType,
      params: formToObject(form)
    };

    var res = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      setText(statusEl, "Failed to submit job");
      form.dataset.submitting = "0";
      if (btn) {
        btn.disabled = false;
      }
      return;
    }
    var data = await res.json();
    var jobId = data.job_id;
    setText(statusEl, "Job submitted: " + jobId);
    pollJob(jobId, statusEl, resultEl);
    form.dataset.submitting = "0";
    if (btn) {
      btn.disabled = false;
    }
  }

  async function cancelJob(form) {
    if (form.dataset.submitting === "1") {
      return;
    }
    form.dataset.submitting = "1";
    var btn = form.querySelector("button[type='submit']");
    if (btn) {
      btn.disabled = true;
    }

    var statusEl = q(form.dataset.statusTarget || "cancel-status");
    var input = form.querySelector("input[name='job_id']");
    var jobId = input ? input.value.trim() : "";
    if (!jobId) {
      setText(statusEl, "Job id required");
      form.dataset.submitting = "0";
      if (btn) {
        btn.disabled = false;
      }
      return;
    }
    setText(statusEl, "Sending cancel request...");
    var res = await fetch("/api/jobs/" + jobId + "/cancel", { method: "POST" });
    if (!res.ok) {
      setText(statusEl, "Cancel failed");
      form.dataset.submitting = "0";
      if (btn) {
        btn.disabled = false;
      }
      return;
    }
    var data = await res.json();
    setText(statusEl, "Cancel status: " + data.status);
    loadJobs();
    form.dataset.submitting = "0";
    if (btn) {
      btn.disabled = false;
    }
  }

  function pollJob(jobId, statusEl, resultEl) {
    var timer = setInterval(async function () {
      try {
        var res = await fetch("/api/jobs/" + jobId);
        if (!res.ok) {
          setText(statusEl, "Job not found");
          clearInterval(timer);
          return;
        }
        var job = await res.json();
        setText(statusEl, "Status: " + job.status + " | started: " + (job.started_at || "-") + " | finished: " + (job.finished_at || "-"));
        if (job.result) {
          setText(resultEl, JSON.stringify(job.result, null, 2));
        }
        if (job.status === "error") {
          setText(resultEl, job.error || "unknown error");
          clearInterval(timer);
        }
        if (job.status === "done") {
          clearInterval(timer);
        }
      } catch (err) {
        setText(statusEl, "Polling error: " + err);
        clearInterval(timer);
      }
    }, 2000);
  }

  function bindForms() {
    var forms = document.querySelectorAll("form.job-form");
    forms.forEach(function (form) {
      form.addEventListener("submit", function (ev) {
        ev.preventDefault();
        submitJob(form);
      });
    });
  }

  function bindCancelForms() {
    var forms = document.querySelectorAll("form.cancel-form");
    forms.forEach(function (form) {
      form.addEventListener("submit", async function (ev) {
        ev.preventDefault();
        cancelJob(form);
      });
    });
  }

  async function loadJobs() {
    var listEl = q("jobs-list");
    if (!listEl) {
      return;
    }
    var res = await fetch("/api/jobs");
    if (!res.ok) {
      listEl.textContent = "Failed to load jobs";
      return;
    }
    var data = await res.json();
    var items = data.jobs || [];
    if (!items.length) {
      listEl.textContent = "No jobs yet";
      return;
    }
    var lines = items.map(function (job) {
      var cycle = "";
      if (job.result && job.result.last_summary && job.result.last_summary.cycle) {
        cycle = " | cycle " + job.result.last_summary.cycle;
      }
      var lastCsv = "";
      if (job.result && job.result.last_summary && job.result.last_summary.optimize_result) {
        lastCsv = job.result.last_summary.optimize_result.result_csv || "";
        if (lastCsv) {
          lastCsv = " | csv " + lastCsv;
        }
      }
      return job.id + " | " + job.type + " | " + job.status + cycle + " | " + job.created_at + lastCsv;
    });
    listEl.textContent = lines.join("\n");
  }

  async function loadInspirations() {
    var listEl = q("inspiration-list");
    if (!listEl) {
      return;
    }
    var res = await fetch("/api/inspirations");
    if (!res.ok) {
      listEl.textContent = "Failed to load inspirations";
      return;
    }
    var data = await res.json();
    var items = data.items || [];
    if (!items.length) {
      listEl.textContent = "No inspirations yet";
      return;
    }
    var lines = items.map(function (item) {
      return item.created_at + " | " + item.text;
    });
    listEl.textContent = lines.join("\n\n");
  }

  async function loadLogs() {
    var select = q("log-select");
    var scopeEl = q("log-scope");
    if (!select || !scopeEl) {
      return;
    }
    var scope = scopeEl.value || "auto_runs";
    var res = await fetch("/api/logs?scope=" + encodeURIComponent(scope));
    if (!res.ok) {
      select.innerHTML = "";
      return;
    }
    var data = await res.json();
    var logs = data.logs || [];
    select.innerHTML = "";
    logs.forEach(function (item) {
      var opt = document.createElement("option");
      opt.value = item.path;
      opt.textContent = item.path + " (" + item.size + " bytes)";
      select.appendChild(opt);
    });
  }

  async function viewLog() {
    var select = q("log-select");
    var tailEl = q("log-tail");
    var contentEl = q("log-content");
    if (!select || !contentEl) {
      return;
    }
    var path = select.value || "";
    if (!path) {
      contentEl.textContent = "请选择日志文件";
      return;
    }
    var tail = tailEl ? tailEl.value : "200";
    var res = await fetch(
      "/api/logs/content?path=" + encodeURIComponent(path) + "&tail=" + encodeURIComponent(tail || "200")
    );
    if (!res.ok) {
      contentEl.textContent = "读取失败";
      return;
    }
    var data = await res.json();
    var header = "[path] " + data.path + "\\n[mtime] " + data.mtime + "\\n[truncated] " + data.truncated + "\\n\\n";
    contentEl.textContent = header + (data.content || "");
  }

  function bindLogControls() {
    var scopeEl = q("log-scope");
    var refreshBtn = q("log-refresh");
    var viewBtn = q("log-view");
    if (scopeEl) {
      scopeEl.addEventListener("change", function () {
        loadLogs();
      });
    }
    if (refreshBtn) {
      refreshBtn.addEventListener("click", function () {
        loadLogs();
      });
    }
    if (viewBtn) {
      viewBtn.addEventListener("click", function () {
        viewLog();
      });
    }
  }

  function init() {
    bindForms();
    bindCancelForms();
    loadJobs();
    loadInspirations();
    loadLogs();
    bindLogControls();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.WQMINER_SUBMIT = function (form) {
    submitJob(form);
    return false;
  };

  window.WQMINER_CANCEL = function (form) {
    cancelJob(form);
    return false;
  };
})();
