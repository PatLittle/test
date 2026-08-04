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

async function query(sql, params = []) {
  return worker.db.query(sql, params);
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

function ownerLink(ownerOrg) {
  const url =
    "https://search.open.canada.ca/briefing_titles/?owner_org=" +
    encodeURIComponent(ownerOrg);
  return externalLink(url, ownerOrg);
}

function trackingLink(ownerOrg, trackingNumber) {
  const url =
    "https://search.open.canada.ca/briefing_titles/record/" +
    encodeURIComponent(ownerOrg) +
    "," +
    encodeURIComponent(trackingNumber);
  return externalLink(url, trackingNumber);
}

function requestLink(ownerOrg, requestNumber) {
  const filters = `owner_org:${ownerOrg}|request_number:${requestNumber}`;
  const url =
    "https://open.canada.ca/data/en/dataset/" +
    "0797e893-751e-4695-8229-a5066e4fe43c/resource/" +
    "19383ca2-b01a-487d-88f7-e1ffbc7d39c2?filters=" +
    encodeURIComponent(filters);
  return externalLink(url, requestNumber);
}

function uidLinks(value) {
  return String(value ?? "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((identifier) =>
      externalLink(
        "https://open.canada.ca/en/search/ati/reference/" +
          encodeURIComponent(identifier),
        identifier,
      ),
    )
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

async function updateStats() {
  const rows = await query("SELECT key, value FROM meta_counts");
  const values = Object.fromEntries(rows.map((row) => [row.key, row.value]));
  const element = document.getElementById("bn-ati-stats");

  const linkA = externalLink(
    "https://open.canada.ca/data/en/dataset/ee9bd7e8-90a5-45db-9287-85c8cf3589b6/resource/299a2e26-5103-4a49-ac3a-53db9fcc06c7",
    "Proactive Disclosure - Briefing Note Titles and Numbers",
  );
  const linkB = externalLink(
    "https://open.canada.ca/data/en/dataset/2916fad5-ebcc-4c86-b0f3-4f619b29f412/resource/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e",
    "Analytics - ATI informal requests per summary",
  );
  const linkC = externalLink(
    "https://open.canada.ca/data/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2",
    "Completed Access to Information Request Summaries dataset",
  );

  element.innerHTML = `
    <div>
      <strong>Summary</strong>
      <br>${linkA}: ${Number(values.A_rows || 0).toLocaleString()}
      <br>${linkB}: ${Number(values.B_rows || 0).toLocaleString()}
      <br>${linkC}: ${Number(values.C_rows || 0).toLocaleString()}
      <br>Joined ATI summaries and informal-request data: ${Number(values.BC_rows || 0).toLocaleString()}
      <br>Briefing-note reference matches in an ATI summary from the same organization: ${Number(values.matches || 0).toLocaleString()}
      <br>Strong matches after weak IDs were removed: ${Number(values.strong_matches || 0).toLocaleString()}
      <br>Weak matches separated for review: ${Number(values.weak_matches || 0).toLocaleString()}
      <br>Open by Default matches: ${Number(values.open_by_default_matches || 0).toLocaleString()}
      <br>DocumentCloud records retained in the persistent cache: ${Number(values.documentcloud_cached_records || 0).toLocaleString()}
    </div>
  `;
}

const sortableColumns = [
  "owner_org",
  "tracking_number",
  "request_number",
  "informal_requests_sum",
  "open_by_default_url",
  "open_by_default_flag",
  "unique_identifiers",
];

const table = new DataTable("#report", {
  serverSide: true,
  processing: true,
  searchDelay: 300,
  scrollX: true,
  pageLength: 25,
  lengthMenu: [10, 25, 50, 100],
  order: [[0, "asc"]],
  ajax: async (request, callback) => {
    try {
      const start = Number(request.start || 0);
      const length = Math.min(Number(request.length || 25), 100);
      const orderIndex = Number(request.order?.[0]?.column || 0);
      const orderColumn = sortableColumns[orderIndex] || "owner_org";
      const orderDirection =
        String(request.order?.[0]?.dir).toLowerCase() === "desc" ? "DESC" : "ASC";
      const search = String(request.search?.value || "").trim();

      const where = search
        ? `WHERE owner_org LIKE ? OR tracking_number LIKE ? OR request_number LIKE ?
           OR unique_identifiers LIKE ? OR open_by_default_url LIKE ?`
        : "";
      const searchParams = search ? Array(5).fill(`%${search}%`) : [];

      const totalRows = await query(
        "SELECT COUNT(*) AS count FROM strong_matches",
      );
      const filteredRows = await query(
        `SELECT COUNT(*) AS count FROM strong_matches ${where}`,
        searchParams,
      );
      const rows = await query(
        `SELECT
           id,
           owner_org,
           tracking_number,
           request_number,
           informal_requests_sum,
           open_by_default_url,
           open_by_default_flag,
           unique_identifiers
         FROM strong_matches
         ${where}
         ORDER BY ${orderColumn} ${orderDirection}
         LIMIT ? OFFSET ?`,
        [...searchParams, length, start],
      );

      callback({
        draw: request.draw,
        recordsTotal: Number(totalRows[0]?.count || 0),
        recordsFiltered: Number(filteredRows[0]?.count || 0),
        data: rows,
      });
    } catch (error) {
      console.error(error);
      callback({
        draw: request.draw,
        recordsTotal: 0,
        recordsFiltered: 0,
        data: [],
      });
    }
  },
  columns: [
    {
      title: "Organization",
      data: "owner_org",
      render: (value, type) =>
        type === "display" ? ownerLink(value) : value,
    },
    {
      title: "Tracking number",
      data: "tracking_number",
      render: (value, type, row) =>
        type === "display" ? trackingLink(row.owner_org, value) : value,
    },
    {
      title: "ATI request",
      data: "request_number",
      render: (value, type, row) =>
        type === "display" ? requestLink(row.owner_org, value) : value,
    },
    {
      title: "Informal requests",
      data: "informal_requests_sum",
      className: "dt-right",
      render: (value, type) =>
        type === "display" ? Number(value || 0).toLocaleString() : Number(value || 0),
    },
    {
      title: "Open by Default",
      data: "open_by_default_url",
      orderable: false,
      render: (value, type) =>
        type === "display" ? openByDefaultLinks(value) : value,
    },
    {
      title: "Available",
      data: "open_by_default_flag",
      className: "dt-center",
      render: (value, type) =>
        type === "display" && Number(value)
          ? '<span class="open-check" aria-label="Available through Open by Default">✓</span>'
          : Number(value),
    },
    {
      title: "ATI reference",
      data: "unique_identifiers",
      orderable: false,
      render: (value, type) =>
        type === "display" ? uidLinks(value) : value,
    },
  ],
});

document.querySelector("#report tbody").addEventListener("click", async (event) => {
  if (event.target.closest("a")) return;

  const tableRow = event.target.closest("tr");
  if (!tableRow) return;

  const row = table.row(tableRow);
  if (!row.data()) return;

  if (row.child.isShown()) {
    row.child.hide();
    tableRow.classList.remove("shown");
    return;
  }

  row.child('<div class="dt-details">Loading full record…</div>').show();
  tableRow.classList.add("shown");

  const details = await query(
    `SELECT *
     FROM strong_matches
     WHERE id = ?
     LIMIT 1`,
    [row.data().id],
  );
  const record = details[0] || {};

  row.child(`
    <div class="dt-details">
      <gcds-heading tag="h3">Full record</gcds-heading>
      <dl>
        <dt>Organization</dt>
        <dd>${ownerLink(record.owner_org || "")}</dd>
        <dt>Tracking number</dt>
        <dd>${trackingLink(record.owner_org || "", record.tracking_number || "")}</dd>
        <dt>ATI request</dt>
        <dd>${requestLink(record.owner_org || "", record.request_number || "")}</dd>
        <dt>Informal request total</dt>
        <dd>${Number(record.informal_requests_sum || 0).toLocaleString()}</dd>
        <dt>ATI reference</dt>
        <dd>${uidLinks(record.unique_identifiers || "")}</dd>
        <dt>Summary (English)</dt>
        <dd>${escapeHtml(record.summary_en || "")}</dd>
        <dt>Summary (French)</dt>
        <dd>${escapeHtml(record.summary_fr || "")}</dd>
        <dt>Open by Default URL</dt>
        <dd>${openByDefaultLinks(record.open_by_default_url || "") || "Not found"}</dd>
        <dt>DocumentCloud ID</dt>
        <dd>${escapeHtml(record.documentcloud_id || "")}</dd>
        <dt>DocumentCloud title</dt>
        <dd>${escapeHtml(record.documentcloud_title || "")}</dd>
        <dt>DocumentCloud description</dt>
        <dd>${escapeHtml(record.documentcloud_description || "")}</dd>
        <dt>DocumentCloud source</dt>
        <dd>${escapeHtml(record.documentcloud_source || "")}</dd>
        <dt>DocumentCloud created</dt>
        <dd>${escapeHtml(record.documentcloud_created_at || "")}</dd>
        <dt>DocumentCloud updated</dt>
        <dd>${escapeHtml(record.documentcloud_updated_at || "")}</dd>
        <dt>DocumentCloud language</dt>
        <dd>${escapeHtml(record.documentcloud_language || "")}</dd>
        <dt>DocumentCloud metadata</dt>
        <dd><pre>${escapeHtml(record.documentcloud_metadata_json || "")}</pre></dd>
      </dl>
    </div>
  `).show();
});

async function renderWeakSection() {
  const rows = await query(`
    SELECT
      owner_org,
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
    ["c", "c"],
    ["1", "one"],
    ["0", "zero"],
    ["NA", "upper_na"],
    ["na", "lower_na"],
    ["-", "dash"],
    ["REDACTED", "redacted"],
    ["[REDACTED]", "bracketed_redacted"],
    ["TBD-PM-00", "tbd"],
  ];

  const palette = [
    "#26374a",
    "#2b8a3e",
    "#1971c2",
    "#f08c00",
    "#c92a2a",
    "#6741d9",
    "#087f5b",
    "#5f3dc4",
    "#495057",
  ];

  new Chart(document.getElementById("weakChart"), {
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
      interaction: {
        mode: "index",
        intersect: false,
      },
      scales: {
        x: {
          stacked: true,
          ticks: {
            font: { size: 14 },
            maxRotation: 55,
            minRotation: 25,
          },
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: {
            precision: 0,
            font: { size: 14 },
          },
        },
      },
      plugins: {
        legend: {
          position: "top",
          labels: {
            font: { size: 15 },
          },
        },
        title: {
          display: true,
          text: "Weak briefing-note identifiers by organization",
          font: { size: 22 },
        },
      },
    },
  });

  const tbody = document.querySelector("#weakTable tbody");
  const fragment = document.createDocumentFragment();

  for (const row of rows) {
    const tr = document.createElement("tr");
    const values = [
      row.owner_org,
      row.c,
      row.one,
      row.zero,
      row.upper_na,
      row.lower_na,
      row.dash,
      row.redacted,
      row.bracketed_redacted,
      row.tbd,
      row.total,
    ];

    for (const value of values) {
      const td = document.createElement("td");
      td.textContent = String(value ?? "");
      tr.appendChild(td);
    }
    fragment.appendChild(tr);
  }

  tbody.replaceChildren(fragment);
}

await Promise.all([updateStats(), renderWeakSection()]);
