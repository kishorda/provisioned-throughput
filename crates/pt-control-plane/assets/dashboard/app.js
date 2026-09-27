// Provisioned Throughput dashboards (ADR-043). Plain JS, no external libraries.
// The page's <body data-page="system|usage"> picks what to render. The API key stays in
// sessionStorage, so it's forgotten when the tab closes.
"use strict";

const PT = (() => {
  const $ = (sel, root = document) => root.querySelector(sel);
  const esc = (v) =>
    String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

  const store = {
    get(k) { try { return sessionStorage.getItem(k); } catch { return null; } },
    set(k, v) { try { sessionStorage.setItem(k, v); } catch { /* storage blocked */ } },
    del(k) { try { sessionStorage.removeItem(k); } catch { /* storage blocked */ } },
  };
  const prefs = {
    get(k) { try { return localStorage.getItem(k); } catch { return null; } },
    set(k, v) { try { localStorage.setItem(k, v); } catch { /* storage blocked */ } },
  };

  // ---- formatting ----
  const num = (v, d = 0) =>
    v == null || Number.isNaN(v) ? "–" : Number(v).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: d });
  const pct = (v, d = 1) => (v == null ? "–" : `${num(v, d)}%`);
  const money = (cents, currency = "USD") =>
    cents == null ? "–" : (cents / 100).toLocaleString(undefined, { style: "currency", currency });
  const ms = (v) => (v == null ? "–" : v >= 1000 ? `${num(v / 1000, 2)} s` : `${num(v, 0)} ms`);
  const compact = (v) =>
    v == null ? "–" : Number(v).toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 });
  const when = (t) => (t ? new Date(t).toLocaleString() : "–");
  const ago = (secs) => (secs < 90 ? `${secs} s ago` : secs < 5400 ? `${Math.round(secs / 60)} min ago` : `${Math.round(secs / 3600)} h ago`);
  const pill = (text, cls) => `<span class="pill ${esc(cls)}">${esc(text)}</span>`;
  const bar = (used, total, cls = "") => {
    const f = total > 0 ? used / total : 0;
    const c = cls || (f > 1 ? "over" : f >= 0.9 ? "hi" : "");
    return `<div class="bar" title="${num(used, 2)} of ${num(total, 2)}"><span class="${c}" style="width:${Math.min(100, f * 100).toFixed(1)}%"></span></div>`;
  };

  // An SVG sparkline of `values` (nulls are gaps), with an optional reference line.
  function spark(values, { ref = null, max = null, title = "" } = {}) {
    const w = 300, h = 48, pad = 3;
    const nums = values.filter((v) => v != null);
    const top = Math.max(max ?? 0, ref ?? 0, ...nums, 1e-9);
    const x = (i) => (values.length <= 1 ? w / 2 : (i / (values.length - 1)) * w);
    const y = (v) => h - pad - (v / top) * (h - 2 * pad);
    let line = "", area = "", run = [];
    const flush = () => {
      if (!run.length) return;
      line += run.map(([i, v], k) => `${k ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
      area += `M${x(run[0][0]).toFixed(1)},${h}` + run.map(([i, v]) => `L${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("") + `L${x(run[run.length - 1][0]).toFixed(1)},${h}Z`;
      run = [];
    };
    values.forEach((v, i) => (v == null ? flush() : run.push([i, v])));
    flush();
    const refLine = ref != null ? `<line class="ref" x1="0" x2="${w}" y1="${y(ref).toFixed(1)}" y2="${y(ref).toFixed(1)}"/>` : "";
    return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img" aria-label="${esc(title)}"><title>${esc(title)}</title><path class="area" d="${area}"/><path class="line" d="${line}"/>${refLine}</svg>`;
  }

  // ---- API ----
  class AuthError extends Error {}
  async function api(path, key) {
    const r = await fetch(path, { headers: { authorization: `Bearer ${key}` } });
    if (r.status === 401) throw new AuthError("That key isn't valid here.");
    const body = await r.json().catch(() => null);
    if (!r.ok) throw new Error(body?.error?.message || `${r.status} ${r.statusText}`);
    return body;
  }

  // ---- theme ----
  function themeToggle(button) {
    const apply = (t) => {
      if (t) document.documentElement.dataset.theme = t;
      else delete document.documentElement.dataset.theme;
      button.textContent = t === "dark" ? "Dark" : t === "light" ? "Light" : "Auto theme";
    };
    apply(prefs.get("pt-theme"));
    button.addEventListener("click", () => {
      const next = { null: "light", light: "dark", dark: null }[document.documentElement.dataset.theme ?? null];
      next ? prefs.set("pt-theme", next) : prefs.set("pt-theme", "");
      apply(next);
    });
  }

  // ---- login + refresh loop shared by both pages ----
  function shell({ keyName, prompt, load, render }) {
    const main = $("main");
    const status = $("#status");
    let timer = null;

    function login(message = "") {
      clearInterval(timer);
      $("#logout").hidden = true;
      main.innerHTML = `<div class="panel login"><h2>Sign in</h2><p class="muted">${esc(prompt)}</p>
        <form id="login"><input id="key" type="password" autocomplete="off" placeholder="API key" required>
        <button class="primary">Open dashboard</button></form><p class="error">${esc(message)}</p></div>`;
      $("#login").addEventListener("submit", (e) => {
        e.preventDefault();
        store.set(keyName, $("#key").value.trim());
        start();
      });
      $("#key").focus();
    }

    async function refresh() {
      const key = store.get(keyName);
      if (!key) return login();
      try {
        const data = await load(key);
        render(main, data, key);
        status.textContent = `Updated ${new Date().toLocaleTimeString()}`;
        status.className = "muted small";
      } catch (e) {
        if (e instanceof AuthError) {
          store.del(keyName);
          return login(e.message);
        }
        status.textContent = `Refresh failed: ${e.message}`;
        status.className = "error small";
      }
    }

    function start() {
      $("#logout").hidden = false;
      main.innerHTML = `<p class="muted">Loading…</p>`;
      refresh();
      clearInterval(timer);
      timer = setInterval(() => $("#auto").checked && refresh(), 15000);
    }

    $("#logout").addEventListener("click", () => { store.del(keyName); login(); });
    $("#refresh").addEventListener("click", refresh);
    themeToggle($("#theme"));
    store.get(keyName) ? start() : login();
    return { refresh };
  }

  // ---- usage view (customer page and the operator's Customers tab) ----
  function usageView(u) {
    if (!u.reservations.length) {
      return `<div class="panel"><p class="muted">No reservations.</p></div>` + invoices(u.invoices);
    }
    const period = `${when(u.from)} → ${when(u.to)}`;
    return `<p class="muted small">Usage from ${esc(period)}. SLA figures are month to date.</p>` +
      u.reservations.map(reservation).join("") + invoices(u.invoices);
  }

  function reservation(r) {
    const pt = r.reservation, s = r.usage.summary, sla = r.sla, series = r.usage.series;
    const cur = pt.price?.currency || sla.currency;
    const regions = (pt.effective_regions || pt.regions).map((x) => `${x.region} ${x.cus}`).join(", ");
    const req = s.requests, served = req.provisioned + req.burst + req.spillover;
    const rejected = Object.values(s.rejected).reduce((a, b) => a + b, 0);
    const slaCls = sla.windows === 0 ? "info" : sla.attainment_pct >= sla.target_attainment_pct ? "ok" : "critical";
    const stateCls = pt.state === "active" ? "ok" : pt.state === "pending" ? "info" : "warning";
    const tiles = [
      ["Requests served", compact(served), `${compact(req.provisioned)} provisioned · ${compact(req.burst)} burst · ${compact(req.spillover)} spillover`],
      ["Utilisation", pct(s.utilisation * 100), "of the entitlement, provisioned WU"],
      ["TTFT p95", ms(s.ttft_ms?.p95), `p50 ${ms(s.ttft_ms?.p50)} · p99 ${ms(s.ttft_ms?.p99)}`],
      ["TPOT p95", ms(s.tpot_ms?.p95), `p50 ${ms(s.tpot_ms?.p50)} · p99 ${ms(s.tpot_ms?.p99)}`],
      ["Cache hit rate", s.cache_hit_rate == null ? "–" : pct(s.cache_hit_rate * 100), `declared ${pct(pt.shape.cache_hit_ratio * 100, 0)}`],
      ["Rejected", compact(rejected), rejected ? Object.entries(s.rejected).map(([k, v]) => `${k} ${v}`).join(" · ") : "none"],
      ["SLA attainment", sla.windows ? pct(sla.attainment_pct, 2) : "–", `${sla.windows_met}/${sla.windows} windows · target ${pct(sla.target_attainment_pct)}`],
      ["SLA credit", sla.credit_pct ? `${sla.credit_pct}%` : "none", sla.credit_amount ? money(sla.credit_amount, cur) : "month to date"],
    ].map(([l, v, sub]) => `<div class="tile"><div class="label">${esc(l)}</div><div class="value">${esc(v)}</div><div class="sub">${esc(sub)}</div></div>`).join("");

    const util = series.map((b) => b.utilisation * 100);
    const reqs = series.map((b) => b.requests.provisioned + b.requests.burst + b.requests.spillover);
    const ttft = series.map((b) => b.ttft_ms?.p95 ?? null);
    const tokens = series.map((b) => b.tokens.uncached_prefill + b.tokens.cached_prefill + b.tokens.decode);
    const charts = [
      ["Utilisation %", spark(util, { ref: 100, title: "utilisation" }), `peak ${pct(Math.max(0, ...util))}`],
      ["Requests per bucket", spark(reqs, { title: "requests" }), `peak ${compact(Math.max(0, ...reqs))}`],
      ["TTFT p95", spark(ttft, { title: "TTFT p95" }), `worst ${ms(Math.max(0, ...ttft.filter((v) => v != null)))}`],
      ["Tokens per bucket", spark(tokens, { title: "tokens" }), `total ${compact(tokens.reduce((a, b) => a + b, 0))}`],
    ].map(([l, svg, sub]) => `<div class="tile"><div class="label">${esc(l)}</div>${svg}<div class="sub">${esc(sub)}</div></div>`).join("");

    const shape = r.usage.shape;
    const shapeRows = shape ? [
      ["Input p95", shape.declared.input_p95, shape.observed_input_p95],
      ["Input max", shape.declared.input_max, shape.observed_input_max],
      ["Output p95", shape.declared.output_p95, shape.observed_output_p95],
    ].map(([k, d, o]) => `<tr><td>${k}</td><td class="num">${num(d)}</td><td class="num">${o == null ? "–" : num(o)}${o != null && o > d ? " " + pill("over", "warning") : ""}</td></tr>`).join("") : "";
    const inShape = shape?.in_shape_fraction == null ? "–" : pct(shape.in_shape_fraction * 100);

    const t = s.tokens;
    const rec = r.recommendation;
    const advice = rec ? `<p><strong>${esc(rec.summary)}</strong></p><p class="muted small">Suggested: ${num(rec.cus)} CU ${esc(rec.tier)} in ${esc(rec.region)}, ${money(rec.monthly_price, cur)} a month (now ${num(pt.cus)} CU, ${money(pt.price?.monthly, cur)}).</p>` : `<p class="muted">Not enough traffic yet to size from.</p>`;
    const missed = sla.missed_windows?.length ? `<details><summary class="small">${sla.missed_windows.length} missed windows</summary><div class="scroll"><table><tr><th>Window</th><th>Detail</th></tr>${sla.missed_windows.slice(0, 50).map((w) => `<tr><td>${esc(when(w.start))}</td><td class="small">${esc(JSON.stringify(w))}</td></tr>`).join("")}</table></div></details>` : "";
    const excl = sla.exclusion_windows?.length ? `<p class="muted small">Excluded periods: ${sla.exclusion_windows.map((w) => `${esc(w.reason)} ${esc(when(w.start))} → ${esc(when(w.end))}`).join("; ")}</p>` : "";

    return `<section class="panel">
      <div class="row"><h2>${esc(pt.name)}</h2>${pill(pt.state, stateCls)}${pill(`SLA ${sla.windows ? pct(sla.attainment_pct, 2) : "no windows yet"}`, slaCls)}
        <span class="muted small">${esc(pt.id)}</span></div>
      <p class="muted small">${esc(pt.model)} · ${num(pt.cus)} CU ${esc(pt.tier)} · ${esc(pt.sku)} · ${esc(regions)} · ${money(pt.price?.monthly, cur)}/month · term ends ${esc(when(pt.term_end))}${pt.auto_renew ? " (renews)" : ""}</p>
      <div class="grid">${tiles}</div>
      <h3 style="margin-top:12px">Over time</h3>
      <div class="grid">${charts}</div>
      <div class="cols" style="margin-top:12px">
        <div><h3>Tokens</h3><table>
          <tr><td>Uncached prompt</td><td class="num">${num(t.uncached_prefill)}</td></tr>
          <tr><td>Cached prompt</td><td class="num">${num(t.cached_prefill)}</td></tr>
          <tr><td>Output</td><td class="num">${num(t.decode)}</td></tr>
          <tr><td>Work units</td><td class="num">${num(s.wu_actual)}</td></tr>
          <tr><td>Errors · cancelled</td><td class="num">${num(s.errors)} · ${num(s.cancelled)}</td></tr></table></div>
        <div><h3>Declared shape</h3><table><tr><th></th><th class="num">Declared</th><th class="num">Observed</th></tr>${shapeRows}
          <tr><td>In shape</td><td></td><td class="num">${inShape}</td></tr></table></div>
        <div><h3>Advice</h3>${advice}${excl}${missed}</div>
      </div>
      ${deployments(pt)}
    </section>`;
  }

  function deployments(pt) {
    const rows = pt.deployments.map((d) => `<tr><td>${esc(d.name)}</td><td class="small">${esc(d.id)}</td><td>${(d.api_keys || []).map((k) => pill(k.prefix + "…" + (k.expires_at ? " until " + when(k.expires_at) : ""), k.expires_at ? "warning" : "info")).join("")}</td><td class="num">${d.max_share == null ? "–" : pct(d.max_share * 100, 0)}</td></tr>`).join("");
    return `<details style="margin-top:8px"><summary class="small">${pt.deployments.length} deployment(s) and endpoints</summary><div class="scroll"><table><tr><th>Deployment</th><th>Id</th><th>Keys</th><th class="num">Max share</th></tr>${rows}</table></div>
      <p class="small muted">${(pt.endpoints || []).map((e) => `${esc(e.region)}: ${esc(e.url)}`).join(" · ")}</p></details>`;
  }

  function invoices(list) {
    if (!list?.length) return "";
    const rows = list.map((i) => `<tr><td>${esc(i.period)}</td><td>${pill(i.status, i.status === "final" ? "ok" : "info")}</td>
      <td class="num">${money(i.subtotal, i.currency)}</td><td class="num">${i.credits ? "−" + money(i.credits, i.currency) : "–"}</td><td class="num"><strong>${money(i.total, i.currency)}</strong></td>
      <td><details><summary class="small">${i.lines.length} lines</summary><table>${i.lines.map((l) => `<tr><td class="small">${esc(l.description)}</td><td class="num small">${money(l.amount, i.currency)}</td></tr>`).join("")}</table></details></td></tr>`).join("");
    return `<section class="panel"><h2>Invoices</h2><div class="scroll"><table><tr><th>Month</th><th>Status</th><th class="num">Subtotal</th><th class="num">Credits</th><th class="num">Total</th><th>Lines</th></tr>${rows}</table></div></section>`;
  }

  // ---- system view (operators) ----
  const sevOrder = { critical: 0, warning: 1, info: 2 };
  const healthCls = { healthy: "ok", degraded: "warning", down: "critical", unknown: "info" };

  function systemView(v) {
    const counts = v.alerts.reduce((m, a) => ((m[a.severity] = (m[a.severity] || 0) + 1), m), {});
    const alerts = v.alerts.length
      ? v.alerts.sort((a, b) => sevOrder[a.severity] - sevOrder[b.severity]).map((a) => `<div class="alert">${pill(a.severity, a.severity)}<span class="scope">${esc(a.scope)}</span><span>${esc(a.message)}</span></div>`).join("")
      : `<p>${pill("all clear", "ok")} Nothing needs attention.</p>`;
    const cp = v.control_plane;
    const summary = [
      ["Alerts", `${counts.critical || 0} critical`, `${counts.warning || 0} warning · ${counts.info || 0} info`],
      ["Leader", cp.leader ? cp.leader.holder : "none", cp.leader ? `lease until ${when(cp.leader.expires_at)}` : "background work paused"],
      ["Entitlement version", String(cp.entitlement_version), `signing key ${cp.signing_key}`],
      ["Regions", `${v.regions.filter((r) => r.health === "healthy").length}/${v.regions.length} healthy`, `${v.regions.reduce((a, r) => a + r.serving_gateways, 0)} gateways serving`],
      ["Pools reporting", `${v.pools.filter((p) => !p.stale).length}/${v.pools.length}`, `${v.pools.reduce((a, p) => a + p.report.ready.total, 0)} ready replicas`],
      ["Routers reporting", `${v.routers.filter((r) => !r.stale).length}/${v.routers.length}`, `${v.routers.reduce((a, r) => a + (r.report.workers?.length || 0), 0)} workers`],
    ].map(([l, val, sub]) => `<div class="tile"><div class="label">${esc(l)}</div><div class="value">${esc(val)}</div><div class="sub">${esc(sub)}</div></div>`).join("");

    const regions = v.regions.map((r) => `<tr><td>${esc(r.region)}</td><td>${pill(r.health, healthCls[r.health] || "info")}</td>
      <td class="num">${r.serving_gateways}/${r.gateways}</td><td>${esc(r.serving_since ? when(r.serving_since) : "–")}</td>
      <td>${Object.entries(r.snapshot_key_ids || {}).map(([k, n]) => pill(`${k} ×${n}`, "info")).join("") || "–"}</td>
      <td>${r.open_incidents.map((i) => pill(`${i.source}: ${i.description}`, "warning")).join("") || "–"}</td>
      <td>${r.sales_holds.map((h) => pill(`${h.model} (${h.reason})`, "warning")).join("") || "–"}</td></tr>`).join("");

    const capacity = v.capacity.map((c) => `<tr><td>${esc(c.region)}</td><td>${esc(c.model)}</td><td class="small muted">${esc(c.profile)}</td>
      <td class="num">${num(c.reserved_replicas, 2)} / ${num(c.replicas)}</td><td>${bar(c.reserved_replicas, c.replicas)}</td><td class="num">${pct(c.used_pct)}</td>
      <td class="num">${num(c.reservations)}</td><td class="num">${num(c.cus_sold)}</td>
      <td>${Object.entries(c.free_cus).map(([t, n]) => pill(`${t} ${n}`, n > 0 ? "info" : "warning")).join("")}</td>
      <td class="small">${c.scheduled.map((s) => `+${s.add_replicas} from ${when(s.from)}`).join("<br>") || "–"}</td></tr>`).join("");

    const roles = (r) => (r.prefill || r.decode ? `${r.prefill}P + ${r.decode}D` : `${r.aggregated}`);
    const pools = v.pools.map((p) => {
      const r = p.report;
      const conds = r.conditions.map((c) => {
        const bad = (c.type === "Ready" && c.status !== "True") || (c.type === "CapacityShortfall" && c.status === "True");
        const cls = bad ? "critical" : c.status === "True" ? (c.type === "Ready" ? "ok" : "warning") : "info";
        return c.status === "False" && c.type !== "Ready" ? "" : `<span title="${esc(c.message)}">${pill(`${c.type}${c.reason ? ": " + c.reason : ""}`, cls)}</span>`;
      }).join("");
      const readyCls = r.ready.total < r.min_available.total ? "over" : r.ready.total < r.desired.total ? "hi" : "";
      return `<tr><td>${esc(p.region)}</td><td>${esc(p.id)}</td><td>${esc(r.catalog_model || r.model)}<div class="small muted">${esc(r.engine)} · ${esc(r.profile)}</div></td>
        <td class="num">${r.ready.total} / ${r.desired.total}</td><td>${bar(r.ready.total, r.desired.total || 1, readyCls)}<div class="small muted">${roles(r.ready)} of ${roles(r.desired)}</div></td>
        <td class="num">${roles(r.floor)}</td><td class="num">${roles(r.min_available)}</td>
        <td class="num">${r.hot_spares} hot${r.warm_spares_loaded.total ? ` · ${r.warm_spares_loaded.total} warm loaded` : ""}</td>
        <td class="num">${num(r.allocations)} · ${compact(r.allocated_wu_per_sec)} WU/s${r.failover_wu_per_sec ? `<div class="small">+${compact(r.failover_wu_per_sec)} failover</div>` : ""}</td>
        <td>${r.draining_nodes.length ? `${r.draining_nodes.map(esc).join(", ")}${r.drain_surge.total ? ` (+${r.drain_surge.total} surge)` : ""}` : "–"}</td>
        <td>${conds}</td><td class="small ${p.stale ? "error" : "muted"}">${ago(p.age_secs)}</td></tr>`;
    }).join("");

    const routers = v.routers.map((r) => routerPanel(r)).join("");
    return `
      <section><div class="grid">${summary}</div></section>
      <section class="panel"><h2>Alerts</h2>${alerts}</section>
      <section class="panel"><h2>Regions</h2><div class="scroll"><table><tr><th>Region</th><th>Health</th><th class="num">Gateways serving</th><th>Serving since</th><th>Snapshot keys</th><th>Incidents</th><th>Sales holds</th></tr>${regions}</table></div></section>
      <section class="panel"><h2>Capacity sold</h2><p class="muted small">Replicas each region's pool holds for reservations (including failover headroom), and the CUs still for sale per tier.</p>
        <div class="scroll"><table><tr><th>Region</th><th>Model</th><th>Profile</th><th class="num">Reserved / pool</th><th></th><th class="num">Used</th><th class="num">Reservations</th><th class="num">CUs sold</th><th>Free CUs</th><th>Scheduled</th></tr>${capacity}</table></div></section>
      <section class="panel"><h2>Model pools and replicas</h2><p class="muted small">From each region's capacity controller. P = prefill, D = decode workers.</p>
        ${v.pools.length ? `<div class="scroll"><table><tr><th>Region</th><th>Pool</th><th>Model</th><th class="num">Ready</th><th></th><th class="num">Floor</th><th class="num">Min available</th><th class="num">Spares</th><th class="num">Allocations</th><th>Draining</th><th>Conditions</th><th>Reported</th></tr>${pools}</table></div>` : `<p class="muted">No controller has reported yet.</p>`}</section>
      <section><h2>Routers and workers</h2>${routers || `<div class="panel"><p class="muted">No router has reported yet.</p></div>`}</section>`;
  }

  function routerPanel(r) {
    const s = r.report || {};
    const q = s.queued || {}, d = s.dispatched || {};
    const cls = ["provisioned", "burst", "spillover", "payg"];
    const workers = (s.workers || []).map((w) => {
      const tenants = Object.keys(w.kv_by_reservation || {}).length;
      return `<tr><td>${esc(w.id)}${w.hot_spare ? " " + pill("hot spare", "info") : ""}<div class="small muted">${esc(w.url)}</div></td>
        <td class="num">${w.slots_used} / ${w.slots}</td><td>${bar(w.slots_used, w.slots)}</td>
        <td class="num">${num(w.kv_used)} / ${num(w.kv_blocks)}</td><td>${bar(w.kv_used, w.kv_blocks)}</td>
        <td class="num">${w.backfill_slots} slots · ${num(w.backfill_kv)} KV</td><td class="num">${tenants}</td></tr>`;
    }).join("");
    return `<div class="panel"><div class="row"><h3>${esc(r.id)}</h3><span class="muted small">${esc(r.region)}</span>
        ${s.failover_active ? pill("failover fence up", "warning") : ""}${r.stale ? pill("stale", "warning") : ""}<span class="muted small">reported ${ago(r.age_secs)}</span></div>
      <p class="small">Queued: ${cls.map((c) => `${c} ${num(q[c])}`).join(" · ")} &nbsp;|&nbsp; Dispatched: ${cls.map((c) => `${c} ${compact(d[c])}`).join(" · ")} &nbsp;|&nbsp; Preempted ${num(s.preempted)} &nbsp;|&nbsp; Backfill ratio ${pct((s.backfill_ratio ?? 0) * 100, 0)}${s.expected_provisioned_slots != null ? ` (expect ${num(s.expected_provisioned_slots, 1)} provisioned slots, peak ${num(s.peak_provisioned_slots, 1)})` : ""}</p>
      <div class="scroll"><table><tr><th>Worker</th><th class="num">Slots</th><th></th><th class="num">KV blocks</th><th></th><th class="num">Backfill held</th><th class="num">Reservations</th></tr>${workers}</table></div></div>`;
  }

  // ---- pages ----
  function systemPage() {
    let tab = store.get("pt-tab") || "system";
    let tenant = store.get("pt-tenant") || "";
    let hours = Number(store.get("pt-hours") || 24);
    const shellApi = shell({
      keyName: "pt-operator-key",
      prompt: "Enter the operator API key.",
      async load(key) {
        if (tab === "system") return { tab, view: await api("/internal/v1/dashboard/system", key) };
        const customers = (await api("/internal/v1/dashboard/customers", key)).data;
        if (!customers.some((c) => c.tenant === tenant)) tenant = customers[0]?.tenant || "";
        const view = tenant ? await api(`/internal/v1/dashboard/usage/${encodeURIComponent(tenant)}?hours=${hours}`, key) : null;
        return { tab, customers, view };
      },
      render(main, data) {
        if (data.tab === "system") {
          main.innerHTML = systemView(data.view);
          return;
        }
        const rows = data.customers.map((c) => `<tr data-tenant="${esc(c.tenant)}" style="cursor:pointer"${c.tenant === tenant ? ' class="info"' : ""}><td>${esc(c.tenant)}</td><td class="num">${c.reservations}</td><td class="num">${num(c.cus)}</td><td>${c.models.map(esc).join(", ") || "–"}</td><td class="num">${money(c.monthly, c.currency)}</td></tr>`).join("");
        main.innerHTML = `<section class="panel"><h2>Customers</h2><div class="scroll"><table><tr><th>Customer</th><th class="num">Reservations</th><th class="num">CUs</th><th>Models</th><th class="num">Monthly</th></tr>${rows}</table></div></section>
          <section><div class="row" style="margin-bottom:8px"><h2 style="margin:0">${esc(tenant)}</h2>${hoursPicker(hours)}</div>${data.view ? usageView(data.view) : ""}</section>`;
        main.querySelectorAll("tr[data-tenant]").forEach((tr) => tr.addEventListener("click", () => {
          tenant = tr.dataset.tenant;
          store.set("pt-tenant", tenant);
          shellApi.refresh();
        }));
        bindHours(main, (h) => { hours = h; store.set("pt-hours", h); shellApi.refresh(); });
      },
    });
    document.querySelectorAll(".tabs button").forEach((b) => {
      b.setAttribute("aria-selected", String(b.dataset.tab === tab));
      b.addEventListener("click", () => {
        tab = b.dataset.tab;
        store.set("pt-tab", tab);
        document.querySelectorAll(".tabs button").forEach((x) => x.setAttribute("aria-selected", String(x === b)));
        $("main").innerHTML = `<p class="muted">Loading…</p>`;
        shellApi.refresh();
      });
    });
  }

  const hoursPicker = (h) => `<label class="small muted">Window <select id="hours">${[[1, "1 hour"], [6, "6 hours"], [24, "24 hours"], [168, "7 days"], [720, "30 days"]].map(([v, l]) => `<option value="${v}"${v === h ? " selected" : ""}>${l}</option>`).join("")}</select></label>`;
  const bindHours = (root, on) => $("#hours", root)?.addEventListener("change", (e) => on(Number(e.target.value)));

  function usagePage() {
    let hours = Number(store.get("pt-hours") || 24);
    const shellApi = shell({
      keyName: "pt-tenant-key",
      prompt: "Enter your organisation's admin API key (the key you use for /v1/provisioned-throughput).",
      load: (key) => api(`/v1/dashboard/usage?hours=${hours}`, key),
      render(main, u) {
        main.innerHTML = `<div class="row" style="margin-bottom:8px"><h2 style="margin:0">${esc(u.tenant)}</h2>${hoursPicker(hours)}</div>` + usageView(u);
        bindHours(main, (h) => { hours = h; store.set("pt-hours", h); shellApi.refresh(); });
      },
    });
  }

  return { systemPage, usagePage, spark, esc };
})();

document.addEventListener("DOMContentLoaded", () => {
  const page = document.body.dataset.page;
  if (page === "system") PT.systemPage();
  else if (page === "usage") PT.usagePage();
});
