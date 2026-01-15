#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import os
from datetime import date

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT.parent / "docs"
OUT_HTML = OUT_DIR / "index.html"
TEMPLATE_FILE = ROOT / "templates/index.html"
OUT_SQLITE = OUT_DIR / "data.sqlite"
DEFAULT_DB_URL = "https://raw.githubusercontent.com/OWNER/REPO/BRANCH/data.sqlite"


def inject_script_into_html(html: str, script_tag: str) -> str:
    marker = "<!--REPORT_SCRIPT-->"
    if marker in html:
        return html.replace(marker, script_tag)
    lower = html.lower()
    idx = lower.rfind("</body>")
    if idx != -1:
        return html[:idx] + script_tag + html[idx:]
    return html + script_tag


def main() -> None:
    if not OUT_SQLITE.exists():
        print(f"⚠️  Missing {OUT_SQLITE}; page will require a remote db_url.")
    template_html = TEMPLATE_FILE.read_text(encoding="utf-8")
    db_url = os.environ.get("BN_DB_URL", DEFAULT_DB_URL)
    db_size = str(OUT_SQLITE.stat().st_size) if OUT_SQLITE.exists() else "0"
    loader_js = r"""
<script type=\"module\">
(async () => {
  const statusEl = document.getElementById(\"status\");
  function setStatus(msg, kind = \"info\") {
    if (!statusEl) return;
    statusEl.textContent = msg;
    statusEl.className = `status ${kind}`;
    statusEl.style.display = msg ? \"block\" : \"none\";
  }

  setStatus(\"Loading data…\", \"info\");

  try {
  function addCSS(href){
    return new Promise((resolve,reject)=>{
      if ([...document.styleSheets].some(s => s.href && s.href.includes(href))) return resolve();
      const l=document.createElement('link'); l.rel='stylesheet'; l.href=href;
      l.onload=resolve; l.onerror=reject; document.head.appendChild(l);
    });
  }
  function addScript(src){
    return new Promise((resolve,reject)=>{
      if ([...document.scripts].some(s => s.src && s.src.includes(src))) return resolve();
      const s=document.createElement('script'); s.src=src; s.defer=true;
      s.onload=resolve; s.onerror=reject; document.head.appendChild(s);
    });
  }

  await addCSS(\"https://cdn.datatables.net/2.0.8/css/dataTables.dataTables.min.css\");
  await addScript(\"https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js\");
  await addScript(\"https://cdn.datatables.net/2.0.8/js/dataTables.min.js\");
  await addScript(\"https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js\");
  await addScript(\"https://unpkg.com/sql.js-httpvfs/dist/index.js\");

  const params = new URLSearchParams(location.search);
  const urlParam = params.get(\"db_url\");
  const sizeParam = params.get(\"db_size\");
  const DB_URL = urlParam || \"__DB_URL__\";
  const DB_FILE_LENGTH = Number(sizeParam || \"__DB_FILE_LENGTH__\") || 0;
  const createDbWorker = window.createDbWorker;
  if (!createDbWorker) throw new Error(\"sql.js-httpvfs failed to load\");
  const useFileLength = urlParam ? Number(sizeParam) || 0 : DB_FILE_LENGTH;
  const worker = await createDbWorker(
    [{ from: \"inline\", config: { serverMode: \"full\", requestChunkSize: 4096, url: DB_URL, fileLength: useFileLength || undefined } }],
    \"./sqlite.worker.js\",
    \"./sql-wasm.wasm\"
  );

  async function q(sql, params = []) {
    return await worker.db.query(sql, params);
  }

  function escapeHtml(s){ return String(s ?? '').replace(/[&<>\"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;','\'':'&#39;'}[m])); }
  function linkOwnerOrg(val){
    const raw = String(val || ''); const slug = encodeURIComponent(raw.toLowerCase());
    const url = `https://search.open.canada.ca/briefing_titles/?owner_org=${slug}`;
    return `<a href=\"${url}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(raw)}</a>`;
  }
  function linkUIDs(val){
    const raw = String(val || ''); if (!raw.trim()) return '';
    const parts = raw.split(';').map(s=>s.trim()).filter(Boolean);
    const base = \"https://open.canada.ca/en/search/ati/reference/\";
    return parts.map(id => `<a href=\"${base}${encodeURIComponent(id)}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(id)}</a>`).join(\"<br>\");
  }
  function linkTrackingNumber(owner_org, tracking_number){
    const org = String(owner_org || ''), tn = String(tracking_number || '');
    if (!org || !tn) return escapeHtml(tn);
    const url = `https://search.open.canada.ca/briefing_titles/record/${encodeURIComponent(org)},${encodeURIComponent(tn)}`;
    return `<a href=\"${url}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(tn)}</a>`;
  }
  function linkRequestNumber(owner_org, request_number){
    const org = String(owner_org || ''), rn = String(request_number || '');
    if (!org || !rn) return escapeHtml(rn);
    const filterVal = `owner_org:${org}|request_number:${rn}`;
    const url = \"https://open.canada.ca/data/en/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?filters=\" + encodeURIComponent(filterVal);
    return `<a href=\"${url}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(rn)}</a>`;
  }
  function buildDetails(row){
    const owner = row[0] ?? '', tn = row[1] ?? '', rn = row[2] ?? '';
    const sum = Number(row[3] || 0), openFlag = row[4] ?? 0;
    const openUrl = row[5] ?? '', uids = row[6] ?? '';
    const s_en = row[7] ?? '', s_fr = row[8] ?? '';
    return `
      <div class=\"dt-details\">
        <h4>Full details</h4>
        <p><strong>owner_org:</strong> ${escapeHtml(owner)}</p>
        <p><strong>tracking_number:</strong> ${linkTrackingNumber(owner, tn)}</p>
        <p><strong>request_number:</strong> ${linkRequestNumber(owner, rn)}</p>
        <p><strong>Informal Requests (sum):</strong> ${sum.toLocaleString()}</p>
        <p><strong>Open by default:</strong> ${Number(openFlag) ? "Yes" : "No"}</p>
        <p><strong>Open by default URL:</strong> ${openUrl ? `<a href=\"${escapeHtml(openUrl)}\" target=\"_blank\" rel=\"noopener\">open link</a>` : ""}</p>
        <p><strong>Unique Identifier(s):</strong><br>${linkUIDs(uids)}</p>
        <p><strong>summary_en:</strong><br>${escapeHtml(s_en)}</p>
        <p><strong>summary_fr:</strong><br>${escapeHtml(s_fr)}</p>
      </div>`;
  }

  async function updateStats(){
    const rows = await q(\"SELECT key, value FROM meta_counts\");
    const kv = Object.fromEntries(rows.map(r => [r.key, r.value]));
    const dateEl = document.getElementById('date-modified');
    if (dateEl) dateEl.textContent = `Date modified: ${kv.build_date || ''}`;
    const statsEl = document.getElementById('bn-ati-stats'); if (!statsEl) return;

    const A = Number(kv.A_rows||0).toLocaleString();
    const B = Number(kv.B_rows||0).toLocaleString();
    const C = Number(kv.C_rows||0).toLocaleString();
    const BC = Number(kv.BC_rows||0).toLocaleString();
    const matches = Number(kv.matches||0).toLocaleString();
    const strong = Number(kv.strong_matches||0).toLocaleString();
    const weak = Number(kv.weak_matches||0).toLocaleString();
    const open = Number(kv.open_by_default||0).toLocaleString();

    const linkA = `<a href=\"https://open.canada.ca/data/en/dataset/ee9bd7e8-90a5-45db-9287-85c8cf3589b6/resource/299a2e26-5103-4a49-ac3a-53db9fcc06c7\" target=\"_blank\" rel=\"noopener\">Proactive Disclosure - Briefing Note Titles and Numbers</a>`;
    const linkB = `<a href=\"https://open.canada.ca/data/en/dataset/2916fad5-ebcc-4c86-b0f3-4f619b29f412/resource/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e\" target=\"_blank\" rel=\"noopener\">Analytics - ATI informal requests per summary</a>`;
    const linkC = `<a href=\"https://open.canada.ca/data/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2\" target=\"_blank\" rel=\"noopener\">Completed Access to Information Request Summaries dataset</a>`;

    statsEl.innerHTML = `
      <div class=\"stat-card\"><strong>Sources</strong><br>${linkA}: ${A}<br>${linkB}: ${B}<br>${linkC}: ${C}</div>
      <div class=\"stat-card\"><strong>Join</strong><br>Joined (B+C): ${BC}<br>Matches: ${matches}</div>
      <div class=\"stat-card\"><strong>Matches</strong><br>Strong: ${strong}<br>Weak: ${weak}</div>
      <div class=\"stat-card\"><strong>Open by Default</strong><br>${open}</div>
    `;
  }

  await updateStats();

  const demoLimit = Number(new URLSearchParams(location.search).get(\"demo_limit\") || \"250\");
  const maxRows = Number.isFinite(demoLimit) && demoLimit > 0 ? demoLimit : 0;

  const table = jQuery('#report').DataTable({
    serverSide: true,
    processing: true,
    searching: false,
    scrollX: true,
    lengthMenu: [[10,25,50,100],[10,25,50,100]],
    order: [[1, \"asc\"]],
    ajax: async (data, callback) => {
      const start = data.start || 0;
      const length = data.length || 25;
      const colMap = [null,\"owner_org\",\"tracking_number\",\"request_number\",\"informal_requests_sum\",\"open_by_default_url\",\"open_by_default_flag\",\"unique_identifiers\"];
      const orderCol = colMap[(data.order?.[0]?.column ?? 0)] || \"owner_org\";
      const orderDir = (data.order?.[0]?.dir ?? \"asc\").toUpperCase() === \"DESC\" ? \"DESC\" : \"ASC\";

      const total = await q(\"SELECT COUNT(*) AS c FROM strong_matches\");
      const recordsTotalRaw = total[0]?.c || 0;
      const recordsTotal = maxRows ? Math.min(recordsTotalRaw, maxRows) : recordsTotalRaw;

      let rows = [];
      if (!maxRows || start < maxRows) {
        const safeLength = maxRows ? Math.min(length, maxRows - start) : length;
        rows = await q(
          `SELECT owner_org, tracking_number, request_number, informal_requests_sum, open_by_default_flag, open_by_default_url, unique_identifiers, summary_en, summary_fr
           FROM strong_matches
           ORDER BY ${orderCol} ${orderDir}
           LIMIT ? OFFSET ?`, [safeLength, start]
        );
      }

      const dataRows = rows.map(r => [
        r.owner_org,
        r.tracking_number,
        r.request_number,
        r.informal_requests_sum || 0,
        r.open_by_default_flag || 0,
        r.open_by_default_url || \"\",
        r.unique_identifiers || \"\",
        r.summary_en || \"\",
        r.summary_fr || \"\"
      ]);

      callback({
        draw: data.draw,
        recordsTotal,
        recordsFiltered: recordsTotal,
        data: dataRows
      });
    },
    columns: [
      { title: \"\", data: null, className: \"details-control\", orderable: false, defaultContent: \"\" },
      { title: \"owner_org\", data: 0, render: (d,t)=> t==='display' ? linkOwnerOrg(d) : (d ?? '') },
      { title: \"tracking_number\", data: 1, render: (d,t,row)=> t==='display' ? linkTrackingNumber(row[0], d) : (d ?? '') },
      { title: \"request_number\", data: 2, render: (d,t,row)=> t==='display' ? linkRequestNumber(row[0], d) : (d ?? '') },
      { title: \"Informal Requests (sum)\", data: 3, className: 'dt-right', render:(d,t)=>{ const n = Number(d||0); return t==='display'? n.toLocaleString(): n; } },
      { title: \"Open URL\", data: 5, render:(d,t)=> t==='display' ? (d ? `<a href=\"${escapeHtml(d)}\" target=\"_blank\" rel=\"noopener\">open</a>` : \"\") : (d ?? '') },
      { title: \"✓\", data: 4, className: 'dt-center', render:(d,t)=> t==='display' ? (Number(d) ? \"✓\" : \"\") : Number(d||0) },
      { title: \"UIDs\", data: 6, render:(d,t)=> t==='display' ? linkUIDs(d) : (d ?? '') },
      { title: \"summary_en\", data: 7, visible:false },
      { title: \"summary_fr\", data: 8, visible:false }
    ]
  });

  jQuery('#report tbody').on('click', 'tr', function(){
    const row = table.row(this);
    if (row.child.isShown()) { row.child.hide(); jQuery(this).removeClass('shown'); }
    else { row.child(buildDetails(row.data())).show(); jQuery(this).addClass('shown'); }
  });

  const orgStats = await q(`
    SELECT owner_org, strong_count, open_by_default_count, informal_requests_sum_total
    FROM org_stats
    ORDER BY informal_requests_sum_total DESC
  `);

  const topOwners = orgStats.slice(0, 15);
  const orgLabels = topOwners.map(r => r.owner_org);
  const orgValues = topOwners.map(r => r.informal_requests_sum_total || 0);
  const orgCanvas = document.getElementById(\"orgChart\");
  if (orgCanvas) {
    new Chart(orgCanvas, {
      type: \"bar\",
      data: { labels: orgLabels, datasets: [{ label: \"Informal Requests (sum)\", data: orgValues, backgroundColor: \"#219ebc\" }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        scales: { x: { ticks:{ autoSkip:true, maxRotation:45 } }, y: { beginAtZero:true } },
        plugins: { legend:{ display:false }, title:{ display:true, text:\"Top Owner Orgs by Informal Request Usage\" } }
      }
    });
  }

  const weak = await q(`
    SELECT owner_org, c, one, zero, na_upper, na_lower, dash, redacted, bracket_redacted, tbd, total
    FROM weak_stats
    ORDER BY total DESC
  `);

  const weakTop = weak.slice(0, 30);
  const owners = weakTop.map(r => r.owner_org);
  const series = {
    \"c\": weakTop.map(r => r.c),
    \"1\": weakTop.map(r => r.one),
    \"0\": weakTop.map(r => r.zero),
    \"NA\": weakTop.map(r => r.na_upper),
    \"na\": weakTop.map(r => r.na_lower),
    \"-\": weakTop.map(r => r.dash),
    \"REDACTED\": weakTop.map(r => r.redacted),
    \"[REDACTED]\": weakTop.map(r => r.bracket_redacted),
    \"TBD-PM-00\": weakTop.map(r => r.tbd)
  };

  function colorPalette(n){
    const base = [\"#8ecae6\",\"#219ebc\",\"#023047\",\"#ffb703\",\"#fb8500\",\"#90be6d\",\"#277da1\",\"#577590\",\"#f94144\",\"#f3722c\"];
    if (n<=base.length) return base.slice(0,n);
    const arr=[]; while(arr.length<n) arr.push(...base); return arr.slice(0,n);
  }
  const keys = Object.keys(series);
  const colors = colorPalette(keys.length);
  const datasets = keys.map((k,i)=>({ label:k, data:series[k], backgroundColor:colors[i], stack:\"weak\" }));

  const weakCanvas = document.getElementById(\"weakChart\");
  if (weakCanvas) {
    new Chart(weakCanvas, {
      type: \"bar\",
      data: { labels: owners, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        scales: { x: { stacked:true, ticks:{ autoSkip:true, maxRotation:45 } }, y: { stacked:true, beginAtZero:true } },
        plugins: { legend:{ position:\"top\", labels:{ font:{ size:14 } } }, title:{ display:true, text:\"Weak BN IDs per Owner Org (Top 30)\", font:{ size:18 } } }
      }
    });
  }

  const weakNote = document.getElementById(\"weak-note\");
  const weakTableLimit = 200;
  const weakTableRows = weak.slice(0, weakTableLimit);
  if (weakNote) {
    const msg = weak.length > weakTableRows.length
      ? `Showing top ${weakTableRows.length} of ${weak.length} owner orgs.`
      : `Showing ${weak.length} owner orgs.`;
    weakNote.textContent = msg;
  }

  const tbody = document.querySelector(\"#weakTable tbody\");
  if (tbody){
    for (let i=0; i<weakTableRows.length; i++){
      const tr = document.createElement(\"tr\");
      const total =
        (weakTableRows[i].c||0) + (weakTableRows[i].one||0) + (weakTableRows[i].zero||0) +
        (weakTableRows[i].na_upper||0) + (weakTableRows[i].na_lower||0) + (weakTableRows[i].dash||0) +
        (weakTableRows[i].redacted||0) + (weakTableRows[i].bracket_redacted||0) + (weakTableRows[i].tbd||0);
      const cells = [
        weakTableRows[i].owner_org,
        weakTableRows[i].c||0, weakTableRows[i].one||0, weakTableRows[i].zero||0,
        weakTableRows[i].na_upper||0, weakTableRows[i].na_lower||0, weakTableRows[i].dash||0,
        weakTableRows[i].redacted||0, weakTableRows[i].bracket_redacted||0, weakTableRows[i].tbd||0,
        total
      ];
      for (const c of cells){ const td = document.createElement(\"td\"); td.textContent = c; tr.appendChild(td); }
      tbody.appendChild(tr);
    }
  }
  setStatus(\"Data loaded\", \"ok\");
  } catch (err) {
    console.error(err);
    setStatus(`Failed to load data. ${err?.message || err}`, \"error\");
  }
})();
</script>
"""
    loader_js = loader_js.replace('\\"', '"')
    final_html = inject_script_into_html(
        template_html,
        loader_js.replace("__DB_URL__", db_url).replace("__DB_FILE_LENGTH__", db_size),
    )
    final_html = final_html.replace("{{ build_date }}", date.today().isoformat())
    OUT_HTML.write_text(final_html, encoding="utf-8")
    print(f"🧾 Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
