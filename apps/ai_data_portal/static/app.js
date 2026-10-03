// AI Data Portal & Executive Analytics Frontend Logic

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  loadKpiMetrics();
  loadChartsAndAnomalies();
  loadTelemetry();
  setupSqlChat();
  setupRagChat();
});

// Tab Navigation
function initTabs() {
  const tabButtons = document.querySelectorAll(".tab-btn");
  const tabViews = document.querySelectorAll(".tab-view");

  tabButtons.forEach(btn => {
    btn.addEventListener("click", () => {
      const targetTab = btn.getAttribute("data-tab");

      tabButtons.forEach(b => b.classList.remove("active"));
      tabViews.forEach(v => v.classList.remove("active"));

      btn.classList.add("active");
      const targetView = document.getElementById(`tab-${targetTab}`);
      if (targetView) targetView.classList.add("active");
    });
  });
}

// 1. Load KPI Metrics
async function loadKpiMetrics() {
  try {
    const res = await fetch("/api/kpi_metrics");
    const data = await res.json();

    document.getElementById("kpi-revenue").textContent = `$${data.total_revenue.toLocaleString('en-US', {minimumFractionDigits: 2})}`;
    document.getElementById("kpi-orders").textContent = `${data.total_orders} Orders`;
    document.getElementById("kpi-customers").textContent = `${data.active_customers} Enterprise`;
    document.getElementById("kpi-anomalies").textContent = `${data.streaming_anomalies} Events`;
  } catch (err) {
    console.error("Failed to load KPI metrics:", err);
  }
}

// 2. Load Charts and Streaming Anomalies
async function loadChartsAndAnomalies() {
  try {
    const res = await fetch("/api/chart_data");
    const data = await res.json();

    // Render Revenue Chart
    const revCtx = document.getElementById("revenueChart").getContext("2d");
    new Chart(revCtx, {
      type: "line",
      data: {
        labels: data.daily_revenue.map(d => d.date),
        datasets: [{
          label: "Net Realized Revenue ($)",
          data: data.daily_revenue.map(d => d.revenue),
          borderColor: "#6366F1",
          backgroundColor: "rgba(99, 102, 241, 0.15)",
          borderWidth: 3,
          fill: true,
          tension: 0.35,
          pointBackgroundColor: "#06B6D4",
          pointRadius: 5
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#94A3B8" } },
          y: { grid: { color: "rgba(255,255,255,0.05)" }, ticks: { color: "#94A3B8" } }
        }
      }
    });

    // Render Loyalty Chart
    const loyCtx = document.getElementById("loyaltyChart").getContext("2d");
    const tierLabels = Object.keys(data.loyalty_tiers);
    const tierValues = Object.values(data.loyalty_tiers);

    new Chart(loyCtx, {
      type: "doughnut",
      data: {
        labels: tierLabels,
        datasets: [{
          data: tierValues,
          backgroundColor: ["#8B5CF6", "#F59E0B", "#06B6D4"],
          borderColor: "#0F172A",
          borderWidth: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "bottom",
            labels: { color: "#94A3B8", font: { family: "Plus Jakarta Sans", size: 11 } }
          }
        }
      }
    });

    // Render Streaming Anomalies Table
    const tbody = document.getElementById("stream-anomalies-tbody");
    tbody.innerHTML = "";
    data.recent_anomalies.forEach(evt => {
      const tr = document.createElement("tr");
      const badgeClass = evt.status === "CRITICAL" ? "badge-amber" : "badge-blue";
      tr.innerHTML = `
        <td><code>${evt.event_id}</code></td>
        <td>Device #${evt.device_id}</td>
        <td><strong style="color: #F43F5E;">${evt.reading_value}</strong></td>
        <td>${evt.threshold}</td>
        <td>${evt.timestamp}</td>
        <td><span class="kpi-badge ${badgeClass}">${evt.status}</span></td>
      `;
      tbody.appendChild(tr);
    });

  } catch (err) {
    console.error("Failed to load charts:", err);
  }
}

// 3. Setup Natural Language to SQL Studio
function setupSqlChat() {
  const form = document.getElementById("sql-query-form");
  const input = document.getElementById("sql-prompt-input");
  const resultsArea = document.getElementById("sql-results-area");
  const chips = document.querySelectorAll(".prompt-chips .chip:not(.rag-chip)");

  chips.forEach(chip => {
    chip.addEventListener("click", () => {
      input.value = chip.getAttribute("data-query");
      form.dispatchEvent(new Event("submit"));
    });
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const prompt = input.value.trim();
    if (!prompt) return;

    const submitBtn = document.getElementById("btn-submit-sql");
    submitBtn.textContent = "Executing...";

    try {
      const res = await fetch("/api/sql_chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt })
      });
      const data = await res.json();

      resultsArea.classList.remove("hidden");
      document.getElementById("sql-exec-time").textContent = `${data.execution_time_ms} ms`;
      document.getElementById("sql-bytes-scanned").textContent = data.bytes_scanned;
      document.getElementById("sql-code-display").textContent = data.sql;

      // Populate Table
      const thead = document.getElementById("sql-table-head");
      const tbody = document.getElementById("sql-table-body");
      thead.innerHTML = "";
      tbody.innerHTML = "";

      if (data.rows && data.rows.length > 0) {
        const headers = Object.keys(data.rows[0]);
        const trHead = document.createElement("tr");
        headers.forEach(h => {
          const th = document.createElement("th");
          th.textContent = h.replace(/_/g, " ").toUpperCase();
          trHead.appendChild(th);
        });
        thead.appendChild(trHead);

        data.rows.forEach(row => {
          const tr = document.createElement("tr");
          headers.forEach(h => {
            const td = document.createElement("td");
            td.textContent = row[h];
            tr.appendChild(td);
          });
          tbody.appendChild(tr);
        });
      }

    } catch (err) {
      console.error("SQL query error:", err);
    } finally {
      submitBtn.textContent = "Run Query ↵";
    }
  });

  // Copy SQL
  document.getElementById("btn-copy-sql").addEventListener("click", () => {
    const code = document.getElementById("sql-code-display").textContent;
    navigator.clipboard.writeText(code);
    const btn = document.getElementById("btn-copy-sql");
    btn.textContent = "Copied!";
    setTimeout(() => { btn.textContent = "Copy SQL"; }, 2000);
  });
}

// 4. Setup AI Vector RAG Assistant
function setupRagChat() {
  const form = document.getElementById("rag-query-form");
  const input = document.getElementById("rag-prompt-input");
  const resultsArea = document.getElementById("rag-results-area");
  const chips = document.querySelectorAll(".rag-chip");

  chips.forEach(chip => {
    chip.addEventListener("click", () => {
      input.value = chip.getAttribute("data-query");
      form.dispatchEvent(new Event("submit"));
    });
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const query = input.value.trim();
    if (!query) return;

    const submitBtn = document.getElementById("btn-submit-rag");
    submitBtn.textContent = "Searching Vectors...";

    try {
      const res = await fetch("/api/rag_chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query })
      });
      const data = await res.json();

      resultsArea.classList.remove("hidden");
      document.getElementById("rag-search-time").textContent = `${data.search_latency_ms} ms RRF search`;
      
      // Format Markdown-like bullets in Answer
      const formattedAnswer = data.answer.replace(/\n\n/g, "<br/><br/>").replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
      document.getElementById("rag-answer-body").innerHTML = formattedAnswer;

      // Render Ranked Citation Cards
      const container = document.getElementById("rag-citations-container");
      container.innerHTML = "";

      data.citations.forEach(c => {
        const card = document.createElement("div");
        card.className = "citation-card";
        card.innerHTML = `
          <div class="citation-header">
            <span class="topic-badge">${c.topic}</span>
            <span class="rrf-score-badge">RRF: ${c.rrf_score}</span>
          </div>
          <div class="citation-text">${c.text}</div>
        `;
        container.appendChild(card);
      });

    } catch (err) {
      console.error("RAG query error:", err);
    } finally {
      submitBtn.textContent = "Ask AI ↵";
    }
  });
}

// 5. Load Telemetry & Infrastructure Health
async function loadTelemetry() {
  try {
    const res = await fetch("/api/pipeline_health");
    const data = await res.json();

    const list = document.getElementById("component-health-list");
    list.innerHTML = "";

    data.components.forEach(c => {
      const item = document.createElement("div");
      item.className = "component-item";
      const extraInfo = c.lag || c.storage_used || c.last_run || `${c.chunks} chunks`;
      item.innerHTML = `
        <div>
          <div class="comp-name">${c.name}</div>
          <div class="comp-meta">${extraInfo}</div>
        </div>
        <span class="kpi-badge badge-green">${c.status}</span>
      `;
      list.appendChild(item);
    });
  } catch (err) {
    console.error("Failed to load telemetry:", err);
  }
}
