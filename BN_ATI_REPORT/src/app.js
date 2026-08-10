import { createDbWorker } from "sql.js-httpvfs";
import DataTable from "datatables.net-dt";
import "datatables.net-dt/css/dataTables.dataTables.css";
import Chart from "chart.js/auto";

const workerUrl = new URL("./sqlite.worker.js", import.meta.url).toString();
const wasmUrl = new URL("./sql-wasm.wasm", import.meta.url).toString();
const databaseUrl = new URL("../data.sqlite", import.meta.url).toString();

const worker = await createDbWorker(
  [
    {
      from: "inline",
      config: {
        serverMode: "full",
        requestChunkSize: 4096,
        url: databaseUrl,
      },
    },
  ],
  workerUrl,
  wasmUrl,
);

const CKAN_API = "https://open.canada.ca/data/api/action/datastore_search";
const A_RESOURCE = "299a2e26-5103-4a49-ac3a-53db9fcc06c7";
const B_RESOURCE = "e664cf3d-6cb7-4aaa-adfa-e459c2552e3e";
const C_RESOURCE = "19383ca2-b01a-487d-88f7-e1ffbc7d39c2";

let queryChain = Promise.resolve();
function query(sql, params = []) {
  const operation = queryChain.then(() => worker.db.query(sql, params));
  queryChain = operation.catch(() => undefined);
  return operation;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

function externalLink(url, label) {
  if (!url) return "";
  return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
}

function ownerLink(ownerOrg, label = ownerOrg) {
  const url = "https://search.open.canada.ca/briefing_titles/?owner_org=" + encodeURIComponent(ownerOrg);
  return externalLink(url, label || ownerOrg);
}

function trackingLink(ownerOrg, trackingNumber) {
  const url = "https://search.open.canada.ca/briefing_titles/record/" +
    encodeURIComponent(ownerOrg) + "," + encodeURIComponent(trackingNumber);
  return externalLink(url, trackingNumber);
}

function requestLink(ownerOrg, requestNumber) {
  const filters = `owner_org:${ownerOrg}|request_number:${requestNumber}`;
  const url = "https://open.canada.ca/data/en/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/" +
    "19383ca2-b01a-487d-88f7-e1ffbc7d39c2?filters=" + encodeURIComponent(filters);
  return externalLink(url, requestNumber);
}

function uidLinks(value) {
  return String(value ?? "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((identifier) => externalLink(
      "https://open.canada.ca/en/search/ati/reference/" + encodeURIComponent(identifier),
      identifier,
    ))
    .join("<br>");
}

function openByDefaultLinks(value) {
  return String(value ?? "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((url, index) => externalLink(url, index === 0 ? "Open" : `Open ${index + 1}`))
    .join("<br>");
}

function metricCard({ label, value, href = "", variant = "summary" }) {
  const labelMarkup = href
    ? `<a class="metric-card__link" href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`
    : `<div class="metric-card__label">${escapeHtml(label)}</div>`;
  return `
    <article class="metric-card metric-card--${escapeHtml(variant)}">
      ${labelMarkup}
      <div class="metric-card__value">${Number(value || 0).toLocaleString()}</div>
    </article>
  `;
}

async function updateStats() {
  const rows = await query("SELECT key, value FROM meta_counts");
  const values = Object.fromEntries(rows.map((row) => [row.key, row.value]));
  const inputElement = document.getElementById("open-data-inputs");
  const summaryElement = document.getElementById("summary-metrics");

  if (inputElement) {
    inputElement.innerHTML = [
      metricCard({
        label: "Proactive Disclosure - Briefing Note Titles and Numbers",
        value: values.A_rows || 157131,
        href: "https://open.canada.ca/data/en/dataset/ee9bd7e8-90a5-45db-9287-85c8cf3589b6/resource/299a2e26-5103-4a49-ac3a-53db9fcc06c7",
        variant: "input",
      }),
      metricCard({
        label: "Analytics - ATI informal requests per summary",
        value: values.B_rows || 123914,
        href: "https://open.canada.ca/data/en/dataset/2916fad5-ebcc-4c86-b0f3-4f619b29f412/resource/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e",
        variant: "input",
      }),
      metricCard({
        label: "Completed Access to Information Request Summaries dataset",
        value: values.C_rows || 202385,
        href: "https://open.canada.ca/data/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2",
        variant: "input",
      }),
    ].join("");
  }

  if (summaryElement) {
    summaryElement.innerHTML = [
      metricCard({ label: "Joined ATI summaries and informal-request data", value: values.BC_rows || 202385 }),
      metricCard({ label: "Briefing-note reference matches in an ATI summary from the same organization", value: values.matches || 39867 }),
      metricCard({ label: "Strong matches after weak IDs were removed", value: values.strong_matches || 19518 }),
      metricCard({ label: "Weak matches separated for review", value: values.weak_matches || 20349, variant: "weak" }),
      metricCard({ label: "Open by Default matches", value: values.open_by_default_matches || 11268 }),
      metricCard({ label: "DocumentCloud records retained in the persistent cache", value: values.documentcloud_cached_records || 64850 }),
    ].join("");
  }
}

function summaryPreview(value, length = 20) {
  const text = String(value ?? "").trim();
  return text.length <= length ? text : `${text.slice(0, length)}…`;
}

function debounce(fn, delay = 300) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function buildServerFilters(request) {
  const clauses = [];
  const params = [];
  const globalSearch = String(request.search?.value || "").trim();

  if (globalSearch) {
    clauses.push(`(
      tracking_number LIKE ? OR owner_org LIKE ? OR owner_org_title LIKE ? OR
      summary_en LIKE ? OR request_number LIKE ? OR unique_identifiers LIKE ?
    )`);
    params.push(...Array(6).fill(`%${globalSearch}%`));
  }

  const columnSearches = request.columns?.map((column) => String(column.search?.value || "").trim()) || [];
  if (columnSearches[0]) {
    clauses.push("tracking_number LIKE ?");
    params.push(`%${columnSearches[0]}%`);
  }
  if (columnSearches[1]) {
    clauses.push("(owner_org LIKE ? OR owner_org_title LIKE ?)");
    params.push(`%${columnSearches[1]}%`, `%${columnSearches[1]}%`);
  }
  if (columnSearches[2]) {
    clauses.push("summary_en LIKE ?");
    params.push(`%${columnSearches[2]}%`);
  }
  if (columnSearches[3] === "1" || columnSearches[3] === "0") {
    clauses.push("open_by_default_flag = ?");
    params.push(Number(columnSearches[3]));
  }
  if (columnSearches[4]) {
    const match = columnSearches[4].match(/^\s*(>=|<=|>|<|=)?\s*(\d+(?:\.\d+)?)\s*$/);
    if (match) {
      clauses.push(`informal_requests_sum ${match[1] || "="} ?`);
      params.push(Number(match[2]));
    }
  }
  return { where: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "", params };
}

const sortableColumns = [
  "tracking_number",
  "owner_org_title",
  "summary_en",
  "open_by_default_flag",
  "informal_requests_sum",
  "id",
];

const table = new DataTable("#report", {
  serverSide: true,
  processing: true,
  searchDelay: 350,
  scrollX: false,
  autoWidth: false,
  pageLength: 25,
  lengthMenu: [10, 25, 50, 100],
  order: [[0, "asc"]],
  ajax: async (request, callback) => {
    try {
      const start = Number(request.start || 0);
      const length = Math.min(Number(request.length || 25), 100);
      const orderIndex = Number(request.order?.[0]?.column || 0);
      const orderColumn = sortableColumns[orderIndex] || "tracking_number";
      const orderDirection = String(request.order?.[0]?.dir).toLowerCase() === "desc" ? "DESC" : "ASC";
      const filters = buildServerFilters(request);

      const totalRows = await query("SELECT COUNT(*) AS count FROM strong_matches");
      const filteredRows = await query(`SELECT COUNT(*) AS count FROM strong_matches ${filters.where}`, filters.params);
      const rows = await query(
        `SELECT id, owner_org, owner_org_title, tracking_number, summary_en,
                informal_requests_sum, open_by_default_flag
           FROM strong_matches
           ${filters.where}
           ORDER BY ${orderColumn} ${orderDirection}
           LIMIT ? OFFSET ?`,
        [...filters.params, length, start],
      );

      callback({
        draw: request.draw,
        recordsTotal: Number(totalRows[0]?.count || 0),
        recordsFiltered: Number(filteredRows[0]?.count || 0),
        data: rows,
      });
    } catch (error) {
      console.error(error);
      callback({ draw: request.draw, recordsTotal: 0, recordsFiltered: 0, data: [] });
    }
  },
  columns: [
    {
      title: "Reference number",
      data: "tracking_number",
      width: "16%",
      render: (value, type, row) => type === "display" ? trackingLink(row.owner_org, value) : value,
    },
    {
      title: "Organization",
      data: "owner_org_title",
      width: "26%",
      render: (value, type, row) => {
        const label = value || row.owner_org;
        return type === "display" ? ownerLink(row.owner_org, label) : label;
      },
    },
    {
      title: "ATI summary",
      data: "summary_en",
      width: "28%",
      render: (value, type) => type === "display" ? escapeHtml(summaryPreview(value, 20)) : value,
    },
    {
      title: "Online",
      data: "open_by_default_flag",
      width: "8%",
      className: "dt-center",
      render: (value, type) => type === "display" && Number(value)
        ? '<span class="open-check" aria-label="Available online">✓</span>'
        : type === "display" ? "" : Number(value),
    },
    {
      title: "Informal requests",
      data: "informal_requests_sum",
      width: "12%",
      className: "dt-right",
      render: (value, type) => type === "display" ? Number(value || 0).toLocaleString() : Number(value || 0),
    },
    {
      title: "Details",
      data: "id",
      width: "10%",
      orderable: false,
      searchable: false,
      className: "dt-center",
      render: (value, type) => type === "display"
        ? `<button type="button" class="details-button" data-record-id="${Number(value)}">Details</button>`
        : value,
    },
  ],
  initComplete: function () {
    const api = this.api();
    const thead = document.querySelector("#report thead");
    const filterRow = document.createElement("tr");
    filterRow.className = "column-filters";
    const filterDefinitions = [
      { type: "text", placeholder: "Filter reference" },
      { type: "text", placeholder: "Filter organization" },
      { type: "text", placeholder: "Filter summary" },
      { type: "select" },
      { type: "text", placeholder: "e.g. >=1" },
      { type: "none" },
    ];

    filterDefinitions.forEach((definition, index) => {
      const th = document.createElement("th");
      if (definition.type === "text") {
        const input = document.createElement("input");
        input.type = "search";
        input.className = "column-filter";
        input.placeholder = definition.placeholder;
        input.setAttribute("aria-label", definition.placeholder);
        input.addEventListener("click", (event) => event.stopPropagation());
        const redraw = debounce(() => api.column(index).search(input.value).draw(), 300);
        input.addEventListener("input", redraw);
        th.appendChild(input);
      } else if (definition.type === "select") {
        const select = document.createElement("select");
        select.className = "column-filter";
        select.setAttribute("aria-label", "Filter online availability");
        select.innerHTML = '<option value="">All</option><option value="1">Online</option><option value="0">Not online</option>';
        select.addEventListener("click", (event) => event.stopPropagation());
        select.addEventListener("change", () => api.column(index).search(select.value).draw());
        th.appendChild(select);
      }
      filterRow.appendChild(th);
    });
    thead.appendChild(filterRow);
  },
});

function jsonpRequest(url, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const callbackName = `__ckan_jsonp_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const script = document.createElement("script");
    let settled = false;

    const cleanup = () => {
      delete window[callbackName];
      script.remove();
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error("CKAN JSONP request timed out"));
    }, timeoutMs);

    window[callbackName] = (payload) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      cleanup();
      resolve(payload);
    };
    script.onerror = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      cleanup();
      reject(new Error("CKAN JSONP script failed to load"));
    };

    url.searchParams.set("callback", callbackName);
    script.src = url.toString();
    script.async = true;
    document.head.appendChild(script);
  });
}

async function datastoreSearch(resourceId, filters) {
  const url = new URL(CKAN_API);
  url.searchParams.set("resource_id", resourceId);
  url.searchParams.set("limit", "100");
  url.searchParams.set("filters", JSON.stringify(filters));
  const payload = await jsonpRequest(url);
  if (!payload?.success) throw new Error("CKAN API returned success=false");
  return payload.result?.records || [];
}

function parseDateValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function monthEndDate(yearValue, monthValue) {
  const year = Number(yearValue);
  const month = Number(monthValue);
  if (!year || !month || month < 1 || month > 12) return null;
  return new Date(Date.UTC(year, month, 0));
}

function monthYearLabel(date) {
  return date.toLocaleDateString("en-CA", { year: "numeric", month: "long", timeZone: "UTC" });
}

function addBriefingNoteEvents(events, rows) {
  for (const row of rows) {
    const received = parseDateValue(row.date_received);
    if (received) {
      events.push({
        date: received,
        displayDate: received.toLocaleDateString("en-CA", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" }),
        source: "Briefing note",
        label: "Briefing Note Received",
      });
    }
  }
}

function addInformalRequestEvents(events, rows) {
  const totals = new Map();
  for (const row of rows) {
    const date = monthEndDate(row.Year, row.Month);
    if (!date) continue;
    const key = `${row.Year}-${row.Month}`;
    const current = totals.get(key) || { date, count: 0 };
    current.count += Number(row["Number of Informal Requests"] || 0);
    totals.set(key, current);
  }

  for (const { date, count } of totals.values()) {
    const rounded = Number.isInteger(count) ? count : Number(count.toFixed(2));
    events.push({
      date,
      displayDate: monthYearLabel(date),
      source: "ATI analytics",
      label: `${rounded.toLocaleString()} Informal Request${rounded === 1 ? "" : "s"}`,
    });
  }
}

function addRequestCompletedEvents(events, rows) {
  for (const row of rows) {
    const date = monthEndDate(row.year, row.month);
    if (!date) continue;
    events.push({
      date,
      displayDate: monthYearLabel(date),
      source: "ATI summary",
      label: "Request Completed",
    });
  }
}

function addDocumentCloudEvents(events, record) {
  const fields = [
    ["documentcloud_created_at", "Document created"],
    ["documentcloud_updated_at", "Document updated"],
  ];
  for (const [field, label] of fields) {
    for (const value of String(record[field] || "").split(";").map((item) => item.trim()).filter(Boolean)) {
      const date = parseDateValue(value);
      if (!date) continue;
      events.push({
        date,
        displayDate: date.toLocaleDateString("en-CA", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" }),
        source: "DocumentCloud",
        label,
      });
    }
  }
}

function deduplicateTimelineEvents(events) {
  const seen = new Set();
  return events
    .filter((event) => event.date instanceof Date && !Number.isNaN(event.date.getTime()))
    .filter((event) => {
      const key = `${event.source}|${event.label}|${event.date.toISOString()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => a.date - b.date);
}

async function buildTimeline(record) {
  const events = [];
  addDocumentCloudEvents(events, record);

  const [aResult, bResult, cResult] = await Promise.allSettled([
    datastoreSearch(A_RESOURCE, { owner_org: record.owner_org, tracking_number: record.tracking_number }),
    datastoreSearch(B_RESOURCE, { owner_org: record.owner_org, "Request Number": record.request_number }),
    datastoreSearch(C_RESOURCE, { owner_org: record.owner_org, request_number: record.request_number }),
  ]);
  const errors = [];

  if (aResult.status === "fulfilled") addBriefingNoteEvents(events, aResult.value);
  else errors.push("briefing-note received date");

  if (bResult.status === "fulfilled") addInformalRequestEvents(events, bResult.value);
  else errors.push("informal-request dates");

  if (cResult.status === "fulfilled") addRequestCompletedEvents(events, cResult.value);
  else errors.push("request-completed date");

  return { events: deduplicateTimelineEvents(events), errors };
}

function renderTimeline(events, errors = []) {
  if (!events.length) {
    return `<p class="timeline-status">No dated events were returned for this record.${errors.length ? ` Some CKAN lookups failed: ${escapeHtml(errors.join(", "))}.` : ""}</p>`;
  }

  const rows = [];
  for (let index = 0; index < events.length; index += 4) {
    const rowEvents = events.slice(index, index + 4);
    const items = rowEvents.map((event) => `
      <div class="timeline-event">
        <div class="timeline-event__date">${escapeHtml(event.displayDate || monthYearLabel(event.date))}</div>
        <div class="timeline-event__source">${escapeHtml(event.source)}</div>
        <div class="timeline-event__label">${escapeHtml(event.label)}</div>
      </div>
    `).join("");
    rows.push(`<div class="record-timeline">${items}</div>`);
  }

  const warning = errors.length
    ? `<p class="timeline-status">Some CKAN timeline lookups could not be loaded: ${escapeHtml(errors.join(", "))}.</p>`
    : "";
  return `${warning}${rows.join("")}`;
}

function detailValue(record, field) {
  const value = record[field];
  if (field === "owner_org") return ownerLink(record.owner_org || "", record.owner_org_title || record.owner_org || "");
  if (field === "tracking_number") return trackingLink(record.owner_org || "", value || "");
  if (field === "request_number") return requestLink(record.owner_org || "", value || "");
  if (field === "unique_identifiers") return uidLinks(value || "") || "—";
  if (field === "open_by_default_url") return openByDefaultLinks(value || "") || "Not found";
  if (field === "open_by_default_flag") return Number(value) ? '<span class="open-check">✓ Available online</span>' : "Not found online";
  if (field === "informal_requests_sum") return Number(value || 0).toLocaleString();
  if (field === "documentcloud_metadata_json") return `<pre>${escapeHtml(value || "")}</pre>`;
  return escapeHtml(value || "—");
}

function renderRecordDetails(record) {
  const fields = [
    ["tracking_number", "Briefing note reference number"],
    ["owner_org", "Organization"],
    ["owner_org_title", "Organization title"],
    ["request_number", "ATI request"],
    ["summary_en", "ATI summary (English)"],
    ["summary_fr", "ATI summary (French)"],
    ["informal_requests_sum", "Informal request total"],
    ["unique_identifiers", "ATI reference"],
    ["open_by_default_flag", "Open by Default availability"],
    ["open_by_default_url", "Open by Default URL"],
    ["documentcloud_id", "DocumentCloud ID"],
    ["documentcloud_title", "DocumentCloud title"],
    ["documentcloud_description", "DocumentCloud description"],
    ["documentcloud_source", "DocumentCloud source"],
    ["documentcloud_created_at", "DocumentCloud created"],
    ["documentcloud_updated_at", "DocumentCloud updated"],
    ["documentcloud_language", "DocumentCloud language"],
    ["documentcloud_org_raw", "DocumentCloud organization (raw)"],
    ["documentcloud_org_normalized", "DocumentCloud organization (normalized)"],
    ["documentcloud_org_match_method", "DocumentCloud organization match method"],
    ["documentcloud_metadata_json", "DocumentCloud metadata"],
  ];
  return `
    <section class="timeline-section" aria-labelledby="record-timeline-heading">
      <h3 id="record-timeline-heading">Record timeline</h3>
      <div id="record-timeline-content" aria-live="polite">
        <p class="timeline-status">Loading dates from the Briefing Notes, ATI, analytics and DocumentCloud records…</p>
      </div>
    </section>
    <dl class="record-details-grid">
      ${fields.map(([field, label]) => `<dt>${escapeHtml(label)}</dt><dd>${detailValue(record, field)}</dd>`).join("")}
    </dl>
  `;
}

const dialog = document.getElementById("record-dialog");
const dialogContent = document.getElementById("record-dialog-content");
const dialogTitle = document.getElementById("record-dialog-title");
const dialogClose = document.getElementById("record-dialog-close");

async function openRecordDetails(id) {
  const rows = await query("SELECT * FROM strong_matches WHERE id = ? LIMIT 1", [Number(id)]);
  const record = rows[0];
  if (!record) return;

  dialogTitle.textContent = `Record details — ${record.tracking_number || record.request_number || id}`;
  dialogContent.innerHTML = renderRecordDetails(record);
  if (!dialog.open) dialog.showModal();

  try {
    const timeline = await buildTimeline(record);
    const timelineElement = document.getElementById("record-timeline-content");
    if (timelineElement) timelineElement.innerHTML = renderTimeline(timeline.events, timeline.errors);
  } catch (error) {
    console.error(error);
    const timelineElement = document.getElementById("record-timeline-content");
    if (timelineElement) timelineElement.innerHTML = '<p class="timeline-status">The record details loaded, but timeline dates could not be retrieved from CKAN.</p>';
  }
}

document.querySelector("#report tbody").addEventListener("click", (event) => {
  const button = event.target.closest(".details-button");
  if (!button) return;
  openRecordDetails(button.dataset.recordId).catch(console.error);
});

dialogClose.addEventListener("click", () => dialog.close());
dialog.addEventListener("click", (event) => {
  if (event.target === dialog) dialog.close();
});

async function renderWeakSection() {
  const rows = await query(`
    SELECT owner_org,
      SUM(CASE WHEN lower(tracking_number) = 'c' THEN 1 ELSE 0 END) AS c,
      SUM(CASE WHEN tracking_number = '1' THEN 1 ELSE 0 END) AS one,
      SUM(CASE WHEN tracking_number = '0' THEN 1 ELSE 0 END) AS zero,
      SUM(CASE WHEN tracking_number = 'NA' THEN 1 ELSE 0 END) AS upper_na,
      SUM(CASE WHEN tracking_number = 'na' THEN 1 ELSE 0 END) AS lower_na,
      SUM(CASE WHEN tracking_number = '-' THEN 1 ELSE 0 END) AS dash,
      SUM(CASE WHEN tracking_number = 'REDACTED' THEN 1 ELSE 0 END) AS redacted,
      SUM(CASE WHEN tracking_number = '[REDACTED]' THEN 1 ELSE 0 END) AS bracketed_redacted,
      SUM(CASE WHEN tracking_number = 'TBD-PM-00' THEN 1 ELSE 0 END) AS tbd,
      COUNT(*) AS total
    FROM weak_matches
    GROUP BY owner_org
    ORDER BY total DESC, owner_org
  `);

  const definitions = [
    ["c", "c"], ["1", "one"], ["0", "zero"], ["NA", "upper_na"], ["na", "lower_na"],
    ["-", "dash"], ["REDACTED", "redacted"], ["[REDACTED]", "bracketed_redacted"], ["TBD-PM-00", "tbd"],
  ];
  const palette = ["#26374a", "#2b8a3e", "#1971c2", "#f08c00", "#c92a2a", "#6741d9", "#087f5b", "#5f3dc4", "#495057"];

  const canvas = document.getElementById("weakChart");
  if (canvas) {
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: rows.map((row) => row.owner_org),
        datasets: definitions.map(([label, key], index) => ({
          label,
          data: rows.map((row) => Number(row[key] || 0)),
          backgroundColor: palette[index],
          stack: "weak-identifiers",
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { stacked: true, ticks: { font: { size: 14 }, maxRotation: 55, minRotation: 25 } },
          y: { stacked: true, beginAtZero: true, ticks: { precision: 0, font: { size: 14 } } },
        },
        plugins: {
          legend: { position: "top", labels: { font: { size: 15 } } },
          title: { display: true, text: "Weak briefing-note identifiers by organization", font: { size: 22 } },
        },
      },
    });
  }

  const tbody = document.querySelector("#weakTable tbody");
  if (!tbody) return;
  const fragment = document.createDocumentFragment();
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const value of [row.owner_org, row.c, row.one, row.zero, row.upper_na, row.lower_na, row.dash, row.redacted, row.bracketed_redacted, row.tbd, row.total]) {
      const td = document.createElement("td");
      td.textContent = String(value ?? "");
      tr.appendChild(td);
    }
    fragment.appendChild(tr);
  }
  tbody.replaceChildren(fragment);
}

const startupResults = await Promise.allSettled([updateStats(), renderWeakSection()]);
startupResults.forEach((result) => {
  if (result.status === "rejected") console.error("BN ATI startup section failed", result.reason);
});
