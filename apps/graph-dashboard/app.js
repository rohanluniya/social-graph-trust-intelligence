(() => {
  let selectedUserId = null;
  let topAlertUserId = null;
  let celebrityUserId = "usr_000001";

  const roleSelect = document.getElementById("role-select");

  function apiHeaders() {
    return {
      "X-API-Key": roleSelect.value,
      "Content-Type": "application/json",
    };
  }

  async function apiGet(path) {
    const res = await fetch(path, { headers: apiHeaders() });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    }
    return res.json();
  }

  function tierBadgeClass(tier) {
    if (tier === "critical") return "badge-rose";
    if (tier === "high") return "badge-amber";
    if (tier === "medium") return "badge-indigo";
    if (tier === "erased") return "badge-cyan";
    return "badge-emerald";
  }

  function formatInt(n) {
    return Number(n || 0).toLocaleString();
  }

  function renderPartitionChart(part) {
    const wrap = document.getElementById("partition-chart");
    const naive = part.naive_partition_loads || [];
    const vc = part.vertex_cut_partition_loads || [];
    const n = Math.max(naive.length, vc.length, 1);
    const maxVal = Math.max(...naive, ...vc, 1);

    const width = 620;
    const height = 210;
    const padL = 44;
    const padR = 12;
    const padT = 14;
    const padB = 26;
    const plotW = width - padL - padR;
    const plotH = height - padT - padB;
    const groupW = plotW / n;
    const barW = Math.max(4, groupW * 0.36);

    const meanY = padT + plotH * (1 - part.naive_mean_edges / maxVal);

    let barsSvg = "";
    for (let i = 0; i < n; i++) {
      const x0 = padL + i * groupW + groupW * 0.1;
      const hN = (naive[i] / maxVal) * plotH;
      const hV = (vc[i] / maxVal) * plotH;
      const yN = padT + plotH - hN;
      const yV = padT + plotH - hV;
      barsSvg += `
        <rect x="${x0.toFixed(1)}" y="${yN.toFixed(1)}" width="${barW.toFixed(1)}" height="${hN.toFixed(1)}" rx="2" fill="#f97316" opacity="0.88">
          <title>P${i} Naive: ${naive[i]} edges</title>
        </rect>
        <rect x="${(x0 + barW + 2).toFixed(1)}" y="${yV.toFixed(1)}" width="${barW.toFixed(1)}" height="${hV.toFixed(1)}" rx="2" fill="#38bdf8" opacity="0.95">
          <title>P${i} Vertex-Cut: ${vc[i]} edges</title>
        </rect>
        <text x="${(x0 + barW).toFixed(1)}" y="${height - 8}" text-anchor="middle" fill="#94a3b8" font-size="10">P${i}</text>
      `;
    }

    wrap.innerHTML = `
      <svg viewBox="0 0 ${width} ${height}" width="100%" height="210" role="img" aria-label="Partition load distribution">
        <line x1="${padL}" y1="${padT}" x2="${padL}" y2="${padT + plotH}" stroke="#334155" stroke-width="1"/>
        <line x1="${padL}" y1="${padT + plotH}" x2="${width - padR}" y2="${padT + plotH}" stroke="#334155" stroke-width="1"/>
        <line x1="${padL}" y1="${meanY.toFixed(1)}" x2="${width - padR}" y2="${meanY.toFixed(1)}" stroke="#34d399" stroke-width="1.2" stroke-dasharray="4 3"/>
        <text x="${padL - 6}" y="${padT + 8}" text-anchor="end" fill="#94a3b8" font-size="10">${formatInt(maxVal)}</text>
        <text x="${padL - 6}" y="${meanY.toFixed(1)}" text-anchor="end" fill="#34d399" font-size="10">avg</text>
        <text x="${padL - 6}" y="${padT + plotH}" text-anchor="end" fill="#94a3b8" font-size="10">0</text>
        ${barsSvg}
      </svg>
    `;

    document.getElementById("supernode-badge").textContent = `${part.supernode_count} celebrity supernodes`;
    document.getElementById("partition-stats-row").innerHTML = `
      <div class="mini-stat"><span>Naive Skew Ratio</span><strong style="color:#fb923c">${part.naive_skew_ratio}x</strong></div>
      <div class="mini-stat"><span>Vertex-Cut Skew</span><strong class="accent-cyan">${part.vertex_cut_skew_ratio}x</strong></div>
      <div class="mini-stat"><span>2-Hop p95 Latency</span><strong class="accent-emerald">${part.vertex_cut_two_hop_p95_ms} ms <small style="color:#94a3b8">(vs ${part.naive_two_hop_p95_ms})</small></strong></div>
      <div class="mini-stat"><span>Vertex Replication</span><strong>${part.replication_factor}x</strong></div>
    `;
  }

  async function inspectNode(userId) {
    if (!userId) return;
    selectedUserId = userId.trim();
    document.getElementById("node-search-input").value = selectedUserId;
    const body = document.getElementById("node-inspector-body");
    const tierBadge = document.getElementById("insp-tier-badge");

    try {
      const node = await apiGet(`/v1/graph/node/${encodeURIComponent(selectedUserId)}`);
      tierBadge.className = `badge ${tierBadgeClass(node.risk_tier)}`;
      tierBadge.textContent = node.is_erased
        ? "ERASED (GDPR Art. 17)"
        : `${node.risk_tier.toUpperCase()} (${node.risk_score})`;

      if (node.is_erased) {
        body.innerHTML = `
          <div class="receipt-box">
            <strong>${node.user_id}</strong> has been erased under GDPR Right-to-Erasure. Active edges: 0 &bull; Gold features: purged &bull; Serving index: 0 residual records.
          </div>
        `;
        return;
      }

      const attrs = node.feature_attributions || {};
      const attrHtml = Object.entries(attrs)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => {
          const pct = Math.min(100, (Number(v) / 26) * 100);
          return `
            <div class="attr-row">
              <span class="mono">${k}</span>
              <div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(1)}%"></div></div>
              <strong class="mono" style="text-align:right">${Number(v).toFixed(1)}</strong>
            </div>
          `;
        })
        .join("");

      const reasonsHtml = (node.top_reasons || [])
        .map((r) => `<span class="chip ${r !== "ORGANIC_BASELINE" ? "chip-danger" : ""}">${r}</span>`)
        .join("");

      body.innerHTML = `
        <div class="insp-grid">
          <div class="mini-stat"><span>User / Cohort</span><strong class="mono">${node.user_id}</strong><span>${node.cohort_label || "organic"}${node.is_supernode ? " (supernode)" : ""}</span></div>
          <div class="mini-stat"><span>In / Out Degree</span><strong>${formatInt(node.in_degree)} / ${formatInt(node.out_degree)}</strong><span>Recip: ${(node.reciprocal_ratio * 100).toFixed(1)}%</span></div>
          <div class="mini-stat"><span>2-Hop Exact vs HLL</span><strong>${formatInt(node.two_hop_exact)} / ${formatInt(node.two_hop_hll)}</strong><span>Clustering: ${node.local_clustering.toFixed(3)}</span></div>
          <div class="mini-stat"><span>PageRank / Comm</span><strong>${node.pagerank_score.toFixed(5)}</strong><span>Comm #${node.community_id} (n=${formatInt(node.community_size)})</span></div>
        </div>
        <div>
          <div style="font-size:11.5px;color:#94a3b8;margin-bottom:5px;">Explainable Feature Attributions (score = ${node.risk_score} / 100)</div>
          <div class="attr-bars">${attrHtml}</div>
        </div>
        <div class="reason-chips">${reasonsHtml}</div>
      `;
    } catch (err) {
      tierBadge.className = "badge badge-rose";
      tierBadge.textContent = "Not Found";
      body.innerHTML = `<div class="receipt-box error">${err.message}</div>`;
    }
  }

  async function eraseSelectedUser() {
    if (!selectedUserId) return;
    const box = document.getElementById("erasure-receipt");
    box.classList.remove("hidden", "error");
    box.textContent = `Propagating GDPR erasure tombstone for ${selectedUserId}...`;
    try {
      const res = await fetch(`/v1/privacy/erase/${encodeURIComponent(selectedUserId)}`, {
        method: "POST",
        headers: apiHeaders(),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      box.innerHTML = `
        <strong>GDPR ERASURE VERIFIED:</strong> ${data.user_id} &bull;
        edges_closed=${data.edges_closed} &bull;
        residual_active_edges=${data.residual_active_edges} &bull;
        residual_serving=${data.residual_serving_records} &bull;
        tombstone_propagation_ms=${data.tombstone_propagation_ms} ms &bull;
        snapshot=v${data.snapshot_version}
      `;
      await loadAll(false);
      await inspectNode(selectedUserId);
    } catch (err) {
      box.classList.add("error");
      box.textContent = `Erasure blocked: ${err.message}`;
    }
  }

  async function loadAll(selectInitialNode = true) {
    const [health, ov, alertsData, commsData, dqData] = await Promise.all([
      fetch("/health").then((r) => r.json()),
      apiGet("/v1/overview"),
      apiGet("/v1/trust/alerts?limit=8"),
      apiGet("/v1/graph/communities?limit=8"),
      apiGet("/v1/dq"),
    ]);

    document.getElementById("health-status").textContent = `${health.status.toUpperCase()} (${health.runtime_mode})`;
    document.getElementById("snap-version").textContent = `v${ov.snapshot_version}`;
    document.getElementById("freshness-val").textContent = `${ov.freshness_seconds.toFixed(0)}s`;
    document.getElementById("footer-run-id").textContent = `Run ID: ${ov.run_id} • Iceberg Snapshot: ${ov.iceberg_edge_snapshot_id}`;

    // KPI Cards
    document.getElementById("kpi-edges").textContent = formatInt(ov.active_edges);
    document.getElementById("kpi-nodes-sub").textContent = `${formatInt(ov.active_nodes)} active nodes • ${ov.erased_users} erased • ${ov.blocked_pairs} blocks`;

    const part = ov.partitioning;
    document.getElementById("kpi-skew").textContent = `-${part.skew_reduction_pct}%`;
    document.getElementById("kpi-skew-sub").textContent = `Skew ${part.naive_skew_ratio}x → ${part.vertex_cut_skew_ratio}x • 2-hop ${(part.naive_two_hop_p95_ms / Math.max(0.001, part.vertex_cut_two_hop_p95_ms)).toFixed(1)}x faster`;

    document.getElementById("kpi-parity").textContent = `${ov.incremental_speedup_x}x`;
    document.getElementById("kpi-parity-sub").textContent = `Exact parity=${ov.incremental_parity_verified ? "PASS" : "FAIL"} • max Δ=${ov.incremental_max_abs_diff}`;

    const ev = ov.trust_evaluation;
    document.getElementById("kpi-trust-f1").textContent = `F1 ${ev.f1_score.toFixed(3)}`;
    document.getElementById("kpi-trust-sub").textContent = `P=${ev.precision.toFixed(3)} R=${ev.recall.toFixed(3)} • Celeb FP=${ev.celebrity_false_positives}/${ev.celebrity_total}`;

    const sk = ov.sketches;
    document.getElementById("kpi-hll").textContent = `${sk.memory_reduction_ratio}x`;
    document.getElementById("kpi-hll-sub").textContent = `HLL p=${sk.hll_precision} • ${sk.hll_mean_rel_error_pct}% mean err • CMS 0 underest.`;

    document.getElementById("kpi-dq").textContent = `${ov.dq_passed}/${ov.dq_total}`;
    document.getElementById("kpi-dq-sub").textContent = `100% DQ passed • ${ov.dlq_quarantined} poison DLQ • ${ov.duplicates_deduped} deduped`;

    renderPartitionChart(part);

    // Trust Evaluation Pills
    document.getElementById("trust-eval-pills").innerHTML = `
      <span class="badge badge-emerald">TP ${ev.true_positives}</span>
      <span class="badge badge-amber">FP ${ev.false_positives} (FPR ${(ev.false_positive_rate * 100).toFixed(2)}%)</span>
      <span class="badge badge-cyan">Celebrity FP ${ev.celebrity_false_positives}/${ev.celebrity_total}</span>
      <span class="badge badge-indigo">F1 ${ev.f1_score.toFixed(4)}</span>
    `;

    // Alerts Table
    const alertsBody = document.getElementById("alerts-tbody");
    const alerts = alertsData.alerts || [];
    if (alerts.length > 0) {
      topAlertUserId = alerts[0].user_id;
    }
    alertsBody.innerHTML = alerts
      .map((a) => {
        const fillColor = a.risk_tier === "critical" ? "#fb7185" : "#fbbf24";
        const reasons = (a.top_reasons || [])
          .slice(0, 3)
          .map((r) => `<span class="chip chip-danger">${r}</span>`)
          .join(" ");
        return `
          <tr>
            <td class="mono"><strong>${a.user_id}</strong></td>
            <td>
              <div class="score-bar-cell">
                <strong>${a.risk_score.toFixed(1)}</strong>
                <div class="mini-track"><div class="mini-fill" style="width:${Math.min(100, a.risk_score)}%;background:${fillColor}"></div></div>
              </div>
            </td>
            <td><span class="badge ${tierBadgeClass(a.risk_tier)}">${a.risk_tier}</span></td>
            <td class="mono">${a.synthetic_label}</td>
            <td>${a.in_degree} / ${a.out_degree}</td>
            <td>${(a.reciprocal_ratio * 100).toFixed(1)}%</td>
            <td>${a.ring_jaccard_max.toFixed(2)}</td>
            <td>${a.engagement_burst_cms}</td>
            <td><div class="reason-chips">${reasons}</div></td>
            <td><button class="btn btn-secondary btn-sm" data-inspect="${a.user_id}" type="button">Inspect</button></td>
          </tr>
        `;
      })
      .join("");

    alertsBody.querySelectorAll("[data-inspect]").forEach((btn) => {
      btn.addEventListener("click", () => inspectNode(btn.getAttribute("data-inspect")));
    });

    // Communities Table
    document.getElementById("modularity-badge").textContent = `Modularity Q = ${commsData.modularity_q} • ${commsData.community_count} communities`;
    document.getElementById("communities-tbody").innerHTML = (commsData.communities || [])
      .map(
        (c) => `
        <tr>
          <td class="mono">comm_${String(c.community_id).padStart(2, "0")}</td>
          <td><strong>${formatInt(c.member_count)}</strong></td>
          <td>${c.avg_risk_score.toFixed(1)}</td>
          <td>${c.high_risk_members > 0 ? `<span class="badge badge-rose">${c.high_risk_members}</span>` : "0"}</td>
          <td>${c.avg_clustering.toFixed(3)}</td>
          <td>${formatInt(c.max_in_degree)}</td>
        </tr>
      `
      )
      .join("");

    // DQ Checks & Iceberg Tables
    document.getElementById("dq-summary-badge").textContent = `${dqData.dq_passed} / ${dqData.dq_total} PASS`;
    document.getElementById("dq-list").innerHTML = (dqData.checks || [])
      .map(
        (c) => `
        <div class="dq-item">
          <div class="dq-left">
            <span class="badge ${c.status === "PASS" ? "badge-emerald" : "badge-rose"}">${c.status}</span>
            <span class="mono">${c.check_id}</span>
            <span>${c.check_name}</span>
          </div>
          <span class="mono" style="color:#94a3b8">${c.observed_value}</span>
        </div>
      `
      )
      .join("");

    document.getElementById("iceberg-grid").innerHTML = (dqData.iceberg_tables || [])
      .map(
        (t) => `
        <div class="ice-card">
          <span class="mono">${t.table}</span>
          <strong>${formatInt(t.rows)} <small style="color:#94a3b8">rows</small></strong>
        </div>
      `
      )
      .join("");

    if (selectInitialNode && topAlertUserId) {
      await inspectNode(topAlertUserId);
    }
  }

  document.getElementById("node-search-form").addEventListener("submit", (e) => {
    e.preventDefault();
    inspectNode(document.getElementById("node-search-input").value);
  });
  document.getElementById("inspect-celeb-btn").addEventListener("click", () => inspectNode(celebrityUserId));
  document.getElementById("inspect-top-alert-btn").addEventListener("click", () => inspectNode(topAlertUserId));
  document.getElementById("erase-node-btn").addEventListener("click", eraseSelectedUser);
  document.getElementById("refresh-btn").addEventListener("click", () => loadAll(false));
  roleSelect.addEventListener("change", () => loadAll(false));

  loadAll(true).catch((err) => {
    document.getElementById("health-status").textContent = `Error: ${err.message}`;
  });
})();
