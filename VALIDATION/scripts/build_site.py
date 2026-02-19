#!/usr/bin/env python3
"""
Static site generator for CKAN validation reports (versioned):
- v0.2 = `tasks` schema (priority)
- v0.1 = `tables` schema (legacy)

Changes:
- ERA concept removed entirely (no parsing, no chips, no filters)
- Index shows Organization + Dataset/Resource names (EN/FR per toggle)
- Single Errors column that follows the language toggle (EN/FR)
- Dataset links on report page:
  * Registry (edit): https://registry.open.canada.ca/dataset/{dataset_id}
  * Portal:   https://open.canada.ca/data/en/dataset/{dataset_id}
"""

import os, re, json, html, ujson
from datetime import datetime
from collections import Counter, defaultdict

IN_PATH   = os.getenv("VALIDATION_JSONL", "validation_enriched.jsonl")
OUT_DIR   = os.getenv("SITE_DIR", "VALIDATION")
SITE_THEME = os.getenv("SITE_THEME", "both").strip().lower()

# --------------------- Utilities ---------------------

def slugify(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+","-", s or "").strip("-") or "report"

def unwrap(val, max_layers=3):
    seen = 0
    while isinstance(val, str) and seen < max_layers:
        try:
            val = ujson.loads(val)
        except ValueError:
            break
        seen += 1
    return val

def parse_reports(v):
    rep = unwrap(v, 3)
    return rep if isinstance(rep, dict) else {}

STATUS_ALIASES = {
    "passed": "success",
    "pass": "success",
    "ok": "success",
    "succeeded": "success",
    "errored": "failure",
    "failed": "failure",
    "error": "failure",
}

STATUS_DISPLAY = {
    "success": "Success",
    "failure": "Failure",
    "pending": "Pending",
    "running": "Running",
    "queued": "Queued",
    "unknown": "Unknown",
}

STATUS_COLORS = {
    "success": "#2e8540",
    "failure": "#d3080c",
    "pending": "#f9a825",
    "running": "#3f57a6",
    "queued": "#5b5b5b",
    "unknown": "#6c757d",
}

DEFAULT_STATUS_COLOR = "#617d98"
PREFERRED_STATUS_ORDER = ["success", "failure", "pending", "running", "queued", "unknown"]
CHART_JS_URL = "https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"

def normalize_status(value):
    if value is None:
        return "unknown"
    key = str(value).strip().lower()
    if not key:
        return "unknown"
    return STATUS_ALIASES.get(key, key)

def display_status(key):
    return STATUS_DISPLAY.get(key, key.replace("_", " ").title() if key else "Unknown")

def order_status_keys(counter: Counter):
    if not counter:
        return []
    present = {k for k, v in counter.items() if v}
    ordered = [k for k in PREFERRED_STATUS_ORDER if k in present]
    remaining = sorted(present - set(ordered))
    return ordered + remaining

def normalize_url_type(value):
    if value is None:
        return "unknown", "Unknown"
    raw = str(value).strip()
    if not raw:
        return "unknown", "Unknown"
    return raw.lower(), raw

def ensure_unique_slug(base, used):
    slug = base or "organization"
    candidate = slug
    idx = 2
    while not candidate or candidate in used:
        candidate = f"{slug}-{idx}"
        idx += 1
    used.add(candidate)
    return candidate

def normalize_org_name(name):
    return (name or "Unknown").strip() or "Unknown"

# --------------------- Version detection ---------------------

def detect_version(rep_dict):
    if not isinstance(rep_dict, dict): return "unknown"
    # language buckets preferred
    for lang in ("en","fr"):
        blk = rep_dict.get(lang)
        if isinstance(blk, dict):
            if isinstance(blk.get("tasks"), list):  return "v0.2"
            if isinstance(blk.get("tables"), list): return "v0.1"
    # generic holders
    for holder in ("report","data"):
        blk = rep_dict.get(holder)
        if isinstance(blk, dict):
            if isinstance(blk.get("tasks"), list):  return "v0.2"
            if isinstance(blk.get("tables"), list): return "v0.1"
    # very old/odd top-level
    if isinstance(rep_dict.get("tasks"), list):  return "v0.2"
    if isinstance(rep_dict.get("tables"), list): return "v0.1"
    return "unknown"

def extract_lang_v02(rep_dict):
    out = {"en": [], "fr": []}
    if isinstance(rep_dict.get("en"), dict): out["en"] = rep_dict["en"].get("tasks") or []
    if isinstance(rep_dict.get("fr"), dict): out["fr"] = rep_dict["fr"].get("tasks") or []
    if not out["en"] and not out["fr"]:
        for holder in ("report","data"):
            blk = rep_dict.get(holder)
            if isinstance(blk, dict) and isinstance(blk.get("tasks"), list):
                out["en"] = blk["tasks"]; break
        if not out["en"] and isinstance(rep_dict.get("tasks"), list):
            out["en"] = rep_dict["tasks"]
    return out

def extract_lang_v01(rep_dict):
    out = {"en": [], "fr": []}
    if isinstance(rep_dict.get("en"), dict): out["en"] = rep_dict["en"].get("tables") or []
    if isinstance(rep_dict.get("fr"), dict): out["fr"] = rep_dict["fr"].get("tables") or []
    if not out["en"] and not out["fr"]:
        for holder in ("report","data"):
            blk = rep_dict.get(holder)
            if isinstance(blk, dict) and isinstance(blk.get("tables"), list):
                out["en"] = blk["tables"]; break
        if not out["en"] and isinstance(rep_dict.get("tables"), list):
            out["en"] = rep_dict["tables"]
    return out

def agg_v02(tasks):
    if not tasks:
        return {"error_count": 0, "row_count": 0, "valid_all": None, "warning_count": 0}
    err=rows=warns=0; valid_all=True; saw=False
    for t in tasks:
        st=t.get("stats") or {}
        err += st.get("errors") if isinstance(st.get("errors"), int) else len(t.get("errors") or [])
        warns+= st.get("warnings") if isinstance(st.get("warnings"),int) else len(t.get("warnings") or [])
        rows+= st.get("rows") if isinstance(st.get("rows"), int) else 0
        v=t.get("valid")
        if isinstance(v,bool):
            saw=True
            if not v: valid_all=False
    return {"error_count": err, "row_count": rows, "valid_all": (valid_all if saw else None), "warning_count": warns}

def norm_get(table, *keys, default=None):
    for k in keys:
        if k in table: return table[k]
    for k in keys:
        alt = k.replace("-", "_") if "-" in k else k.replace("_","-")
        if alt in table: return table[alt]
    return default

def agg_v01(tables):
    if not tables:
        return {"error_count":0,"row_count":0,"valid_all":None,"warning_count":0}
    err=rows=0; valid_all=True; saw=False
    for t in tables:
        err+= int(norm_get(t,"error-count","error_count",default=0) or 0)
        rows+= int(norm_get(t,"row-count","row_count",default=0) or 0)
        v=t.get("valid")
        if isinstance(v,bool):
            saw=True
            if not v: valid_all=False
    return {"error_count":err,"row_count":rows,"valid_all":(valid_all if saw else None),"warning_count":0}

# --------------------- Load & normalize ---------------------

def read_items(jsonl_path):
    items=[]
    with open(jsonl_path,"r",encoding="utf-8") as f:
        for line in f:
            o=ujson.loads(line)
            rep=parse_reports(o.get("reports"))
            version=detect_version(rep)
            created=(o.get("created") or "").strip()

            if version=="v0.2":
                lang_data=extract_lang_v02(rep)
                en_aggr=agg_v02(lang_data["en"]); fr_aggr=agg_v02(lang_data["fr"])
            elif version=="v0.1":
                lang_data=extract_lang_v01(rep)
                en_aggr=agg_v01(lang_data["en"]); fr_aggr=agg_v01(lang_data["fr"])
            else:
                lang_data={"en":[],"fr":[]}
                en_aggr={"error_count":0,"row_count":0,"valid_all":None,"warning_count":0}
                fr_aggr=en_aggr

            b=lambda v: (None if v is None else bool(v))

            items.append({
                "id": (o.get("id") or o.get("resource_id") or ""),
                "resource_id": o.get("resource_id") or "",
                "created": created,
                "status": o.get("status") or "",
                "version": version,
                # Enriched metadata
                "organization_name": o.get("organization_name") or "",
                "dataset_id": o.get("dataset_id") or "",
                "dataset_title_en": o.get("dataset_title_en") or "",
                "dataset_title_fr": o.get("dataset_title_fr") or "",
                "resource_name_en": o.get("resource_name_en") or "",
                "resource_name_fr": o.get("resource_name_fr") or "",
                "url_type": o.get("url_type") or "",
                # aggregates
                "en": {"errors": en_aggr["error_count"], "rows": en_aggr["row_count"], "valid": b(en_aggr["valid_all"]), "warnings": en_aggr["warning_count"]},
                "fr": {"errors": fr_aggr["error_count"], "rows": fr_aggr["row_count"], "valid": b(fr_aggr["valid_all"]), "warnings": fr_aggr["warning_count"]},
                "rep": rep,
                "lang_data": lang_data,
            })
    return items

def build_org_groups(items):
    used_slugs = set()
    groups = {}
    for it in items:
        org_name = normalize_org_name(it.get("organization_name"))
        group = groups.get(org_name)
        if group is None:
            slug_base = slugify(org_name)
            slug = ensure_unique_slug(slug_base, used_slugs)
            group = {
                "name": org_name,
                "slug": slug,
                "items": [],
                "status_counts": Counter(),
                "url_counts": Counter(),
                "status_by_url": defaultdict(Counter),
                "url_labels": {},
            }
            groups[org_name] = group

        group["items"].append(it)

        status_key = normalize_status(it.get("status"))
        group["status_counts"][status_key] += 1

        url_key, url_label = normalize_url_type(it.get("url_type"))
        group["url_counts"][url_key] += 1
        group["status_by_url"][url_key][status_key] += 1
        if url_label:
            group["url_labels"].setdefault(url_key, url_label)

    org_groups = []
    for group in groups.values():
        items_sorted = sorted(group["items"], key=lambda x: (x.get("created") or ""), reverse=True)
        group["items"] = items_sorted
        group["total"] = len(items_sorted)
        group["latest_created"] = max((x.get("created") or "" for x in items_sorted), default="")
        group["status_order"] = order_status_keys(group["status_counts"])
        group["url_order"] = sorted(
            group["url_counts"],
            key=lambda key: (-group["url_counts"][key], group["url_labels"].get(key, "")),
        )
        org_groups.append(group)

    org_groups.sort(key=lambda g: g["name"].lower())
    return org_groups

def prepare_org_summary(group):
    success = group["status_counts"].get("success", 0)
    failure = group["status_counts"].get("failure", 0)
    other = group["total"] - success - failure
    url_types_count = len(group["url_counts"])

    status_order = group.get("status_order") or []
    if not status_order:
        status_order = ["unknown"]
    status_labels = [display_status(s) for s in status_order]
    status_values = [group["status_counts"].get(s, 0) for s in status_order]
    status_colors = [STATUS_COLORS.get(s, DEFAULT_STATUS_COLOR) for s in status_order]
    status_data_json = json.dumps(
        {
            "labels": status_labels,
            "datasets": [{
                "label": "Reports",
                "data": status_values,
                "backgroundColor": status_colors,
                "hoverOffset": 8,
            }]
        },
        ensure_ascii=False,
    )

    url_order = group.get("url_order") or []
    if not url_order:
        url_order = ["unknown"]
    url_labels = [group["url_labels"].get(u, "Unknown") for u in url_order]
    stacked_datasets = []
    for status_key in status_order:
        data_points = [
            group["status_by_url"].get(u, Counter()).get(status_key, 0)
            for u in url_order
        ]
        if any(data_points):
            stacked_datasets.append({
                "label": display_status(status_key),
                "data": data_points,
                "backgroundColor": STATUS_COLORS.get(status_key, DEFAULT_STATUS_COLOR),
                "stack": "status",
                "borderWidth": 0,
            })
    if not stacked_datasets:
        stacked_datasets.append({
            "label": "Reports",
            "data": [group["url_counts"].get(u, 0) for u in url_order],
            "backgroundColor": DEFAULT_STATUS_COLOR,
            "stack": "status",
            "borderWidth": 0,
        })
    url_chart_json = json.dumps(
        {
            "labels": url_labels,
            "datasets": stacked_datasets,
        },
        ensure_ascii=False,
    )

    status_table_rows = []
    for url_key in url_order:
        label = group["url_labels"].get(url_key, "Unknown")
        counts = group["status_by_url"].get(url_key, Counter())
        total = group["url_counts"].get(url_key, 0)
        status_table_rows.append({
            "label": label,
            "total": total,
            "counts": [counts.get(status_key, 0) for status_key in status_order],
        })

    return {
        "success": success,
        "failure": failure,
        "other": other,
        "url_types_count": url_types_count,
        "status_order": status_order,
        "status_labels": status_labels,
        "status_values": status_values,
        "status_data_json": status_data_json,
        "url_order": url_order,
        "url_labels": url_labels,
        "url_chart_json": url_chart_json,
        "status_table_rows": status_table_rows,
        "latest_created": group.get("latest_created") or "N/A",
    }

# --------------------- UI assets ---------------------

CSS = """
:root{
  --bg:#0f1222; --panel:#161b33; --ink:#e8ecfa; --muted:#9aa4c2; --link:#96b5ff;
  --good:#26c281; --bad:#ff5f6d; --chip:#21284a; --line:rgba(255,255,255,.08)
}
*{box-sizing:border-box}
body{margin:0; background:var(--bg); color:var(--ink); font-family:Inter,system-ui,Segoe UI,Roboto,Arial}
a{color:var(--link); text-decoration:none} a:hover{text-decoration:underline}
.container{max-width:1600px; margin:0 auto; padding:28px}
.header{display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:16px}
.header .actions{margin-left:auto; display:flex; gap:10px; flex-wrap:wrap}
.h1{font-size:20px; font-weight:700; letter-spacing:.2px}
.lang-toggle{display:inline-flex; gap:6px}
.lang-toggle .btn{min-width:44px}
.panel{background:var(--panel); border-radius:16px; box-shadow:0 12px 30px rgba(0,0,0,.25)}
.controls{display:flex; flex-wrap:wrap; gap:10px; padding:14px 16px; border-bottom:1px solid var(--line)}
.controls .grow{flex:1 1 260px}
.input, .select, .btn{
  height:36px; padding:0 10px; border:1px solid var(--line); background:rgba(255,255,255,.05);
  color:var(--ink); border-radius:10px; font-size:14px
}
.btn{cursor:pointer}
.table{width:100%; border-collapse:collapse; font-size:14px; table-layout:auto}
th, td{padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:top}
th{position:sticky; top:0; background:linear-gradient(0deg, rgba(22,27,51,.90), rgba(22,27,51,.95)); z-index:1; white-space:nowrap}
th[data-sort]{cursor:pointer; user-select:none}
.sort-ind{opacity:.9}
.badge{display:inline-flex; align-items:center; gap:6px; padding:2px 8px; border-radius:999px; background:var(--chip); color:var(--ink); font-weight:600; font-size:12px}
.badge.ok{color:var(--good)} .badge.bad{color:var(--bad)} .badge.na{color:var(--muted)}
.kv{display:grid; grid-template-columns:180px 1fr; gap:8px; font-size:14px}
.section{padding:18px}
.subtle{color:var(--muted); font-size:12px}
.pager{display:flex; align-items:center; gap:8px; padding:10px 16px; border-top:1px solid var(--line); justify-content:flex-end}
.table-wrap{max-height:70vh; overflow:auto; border-top:1px solid var(--line)}
.hdr-ctrl{margin-top:6px}
.hdr-ctrl .input, .hdr-ctrl .select{width:100%}
.code{font-family:ui-monospace,Menlo,Consolas,monospace; background:rgba(255,255,255,.06); padding:10px; border-radius:10px; overflow:auto; white-space:pre-wrap}
.small{font-size:12px; color:var(--muted)}
.badge.link{background:rgba(255,255,255,.08)}
.chart-grid{display:grid; gap:16px; grid-template-columns:repeat(auto-fit, minmax(280px,1fr)); margin:16px 0}
.chart-card{padding:18px}
.chart-card h3{margin:0 0 12px; font-size:16px}
.chart-card canvas{width:100%; max-width:100%; height:320px}
.summary-grid{display:grid; gap:12px; grid-template-columns:repeat(auto-fit, minmax(220px,1fr))}
.summary-tile{padding:16px; border-radius:12px; background:rgba(255,255,255,.04)}
.summary-tile .label{font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted)}
.summary-tile .value{font-size:20px; font-weight:600; margin-top:4px}
.org-list{list-style:none; margin:0; padding:0; display:grid; gap:16px}
"""

JS = r"""
let sortState = { key: null, dir: 1 }; // 1 asc, -1 desc
let pageSize = 25;
let currentPage = 1;

const reportTable = document.querySelector('[data-report-table]');
const reportTableBody = reportTable ? reportTable.querySelector('tbody') : null;

function getRows(){ return reportTableBody ? Array.from(reportTableBody.querySelectorAll('tr')) : []; }
function visibleFilteredRows(){ return getRows().filter(r => r.dataset.filtered !== '0'); }

function getCellValue(row, key){
  if(key==='created')     return (row.dataset.created    || '').toLowerCase();
  if(key==='status')      return (row.dataset.status     || '').toLowerCase();
  if(key==='resource')    return (row.dataset.resource   || '').toLowerCase();
  if(key==='organization')return (row.dataset.org        || '').toLowerCase();
  if(key==='version')     return (row.dataset.version    || '').toLowerCase();

  // Dataset sorts by visible language (EN/FR)
  if(key==='dataset'){
    const lang = (localStorage.getItem('vr_lang') || 'en').toLowerCase();
    if(lang === 'fr') return (row.dataset.datasetFr || '').toLowerCase();
    return (row.dataset.datasetEn || '').toLowerCase();
  }

  return row.innerText.toLowerCase();
}

function sortBy(key){
  if(!reportTableBody) return;
  const rows = getRows();
  if(!rows.length) return;
  sortState.dir = (sortState.key === key) ? -sortState.dir : 1;
  sortState.key = key;

  rows.sort((a,b)=>{
    const va = getCellValue(a, key), vb = getCellValue(b, key);
    if(key==='created'){
      const da = Date.parse(va)||0, db = Date.parse(vb)||0;
      if(da!==db) return (da - db) * sortState.dir;
    }
    return va.localeCompare(vb) * sortState.dir;
  });
  rows.forEach(r=>reportTableBody.appendChild(r));
  updateSortIndicators();
  renderPage(1);
}

function updateSortIndicators(){
  if(!reportTable) return;
  reportTable.querySelectorAll('th[data-sort]').forEach(th=>{
    const key=th.dataset.sort; th.querySelector('.sort-ind')?.remove();
    if(sortState.key===key){
      const s=document.createElement('span'); s.className='sort-ind'; s.textContent=sortState.dir===1?' ↑':' ↓'; th.appendChild(s);
    }
  });
}

function applyFilters(){
  if(!reportTableBody) return;
  const q   = (document.querySelector('#q')?.value || '').toLowerCase().trim();
  const rF  = (document.querySelector('#filter-resource')?.value || '').toLowerCase().trim();
  const sF  = (document.querySelector('#filter-status')?.value || '').toLowerCase().trim();
  const cF  = (document.querySelector('#filter-created')?.value || '').toLowerCase().trim();
  const oF  = (document.querySelector('#filter-org')?.value || '').toLowerCase().trim();
  const vF  = (document.querySelector('#filter-version')?.value || '').toLowerCase().trim();

  getRows().forEach(r=>{
    const text = r.innerText.toLowerCase();
    const dres = (r.dataset.resource || '').toLowerCase();
    const dsta = (r.dataset.status   || '').toLowerCase();
    const dcre = (r.dataset.created  || '').toLowerCase();
    const dorg = (r.dataset.org      || '').toLowerCase();
    const dver = (r.dataset.version  || '').toLowerCase();

    let show = true;
    if(q && !text.includes(q)) show = false;
    if(rF && !dres.includes(rF)) show = false;
    if(sF && dsta !== sF) show = false;
    if(cF && !dcre.includes(cF)) show = false;
    if(oF && !dorg.includes(oF)) show = false;
    if(vF && dver !== vF) show = false;

    r.dataset.filtered = show ? '1' : '0';
  });
  renderPage(1);
}
function filterTable(){ applyFilters(); }

function setLang(lang){
  localStorage.setItem('vr_lang', lang);
  document.querySelectorAll('[data-lang]').forEach(el=>{
    el.style.display = (el.dataset.lang===lang) ? '' : 'none';
  });
  document.querySelectorAll('.lang-toggle button').forEach(b=>{
    b.classList.toggle('active', b.dataset.set===lang);
  });
}

function setPageSize(v){
  pageSize = parseInt(v,10)||25;
  if(reportTableBody) renderPage(1);
}
function gotoPrev(){
  if(reportTableBody) renderPage(currentPage-1);
}
function gotoNext(){
  if(reportTableBody) renderPage(currentPage+1);
}

function renderPage(page){
  if(!reportTableBody) return;
  const rows = visibleFilteredRows();
  const total = rows.length;
  const totalPages = Math.max(1, Math.ceil(total/pageSize));
  currentPage = Math.max(1, Math.min(page, totalPages));
  getRows().forEach(r => { r.style.display='none'; });
  const start=(currentPage-1)*pageSize, end=start+pageSize;
  rows.slice(start,end).forEach(r => { r.style.display=''; });
  const info = document.querySelector('#pager-info');
  if(info){
    const shownStart = total ? (start+1) : 0;
    const shownEnd = Math.min(end, total);
    info.textContent = `${shownStart}-${shownEnd} of ${total}`;
  }
}

window.addEventListener('DOMContentLoaded',()=>{
  setLang(localStorage.getItem('vr_lang')||'en');
  ['#q','#filter-resource','#filter-status','#filter-created','#filter-org','#filter-version'].forEach(sel=>{
    const el=document.querySelector(sel); if(!el) return;
    el.addEventListener('input', applyFilters);
    el.addEventListener('change', applyFilters);
  });
  const ps=document.querySelector('#page-size');
  if(ps) ps.addEventListener('change', e=> setPageSize(e.target.value));

  if(reportTableBody){
    getRows().forEach(r=> r.dataset.filtered='1');
    updateSortIndicators();
    setPageSize(document.querySelector('#page-size')?.value || 25);
  }
});
"""

# --------------------- Render helpers ---------------------

def badge_state(ok):
    if ok is True:  return '<span class="badge ok">OK</span>'
    if ok is False: return '<span class="badge bad">FAIL</span>'
    return '<span class="badge na">N/A</span>'

def chip(text, cls=""):
    cls_str = f" {cls}" if cls else ""
    return f'<span class="badge{cls_str}">{html.escape(str(text))}</span>'

def lang_html(en, fr):
    return (f'<span data-lang="en">{html.escape(en or "")}</span>'
            f'<span data-lang="fr" style="display:none">{html.escape(fr or "")}</span>')

def render_report_row(it, link_prefix="reports"):
    slug = slugify(it['id'])
    prefix = (link_prefix or "").rstrip("/")
    link = f"{prefix}/{slug}.html" if prefix else f"{slug}.html"
    created = it['created'] or ''
    status  = it['status'] or ''
    resource_code = it['resource_id'] or '-'
    version = it['version'] or 'unknown'
    org     = it['organization_name'] or ''
    d_en, d_fr = it.get("dataset_title_en",""), it.get("dataset_title_fr","")
    r_en, r_fr = it.get("resource_name_en",""), it.get("resource_name_fr","")

    en_err = f"{it['en']['errors']} err"
    fr_err = f"{it['fr']['errors']} err."
    err_cell = f'<span data-lang="en">{en_err}</span><span data-lang="fr" style="display:none">{fr_err}</span>'

    ver_chip= chip(version, "na")
    st_chip = chip(status, 'na' if status not in ('success','failure') else ('ok' if status=='success' else 'bad'))

    dataset_cell = f'{lang_html(d_en, d_fr)}<div class="small"><code>{html.escape(it["dataset_id"])}</code></div>'
    resource_cell= f'{lang_html(r_en, r_fr)}<div class="small"><code>{html.escape(resource_code)}</code> · {html.escape(it.get("url_type") or "")}</div>'

    return f"""
      <tr data-created="{html.escape(created)}"
          data-status="{html.escape(status.lower())}"
          data-resource="{html.escape(resource_code)}"
          data-organization="{html.escape(org)}"
          data-org="{html.escape(org)}"
          data-version="{html.escape(version.lower())}"
          data-dataset-en="{html.escape(d_en)}"
          data-dataset-fr="{html.escape(d_fr)}"
          data-filtered="1">
        <td>
          <a href="{link}"><code>{html.escape(it['id'])}</code></a>
          <div class="subtle">{ver_chip}</div>
        </td>
        <td>{dataset_cell}</td>
        <td>{resource_cell}</td>
        <td>{html.escape(org)}</td>
        <td>{err_cell}</td>
        <td>{st_chip}</td>
        <td><time>{html.escape(created)}</time></td>
      </tr>
    """

# --------------------- Index page ---------------------

def write_index(items, out_dir):
    rows = [render_report_row(it) for it in items]

    html_index = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Validation Reports</title>
<link rel="stylesheet" href="style.css"><script defer src="app.js"></script>
</head><body>
  <div class="container">
    <div class="header">
      <div class="h1">Validation Reports</div>
      <div class="actions">
        <a class="btn" href="organizations/index.html">Organizations</a>
        <div class="lang-toggle">
          <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
          <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="controls">
        <div class="grow"><input class="input" id="q" placeholder="Search all columns…"/></div>
        <div><label class="subtle">Version<br/><select class="select" id="filter-version">
          <option value="">All</option>
          <option value="v0.2">v0.2</option>
          <option value="v0.1">v0.1</option>
          <option value="unknown">unknown</option>
        </select></label></div>
      </div>

      <div class="table-wrap">
        <table class="table" data-report-table>
          <thead>
            <tr>
              <th>ID</th>

              <th data-sort="dataset" onclick="sortBy('dataset')">
                Dataset<br/>
                <div class="small">Title (EN/FR)</div>
              </th>

              <th data-sort="resource" onclick="sortBy('resource')">
                Resource
                <div class="hdr-ctrl"><input class="input" id="filter-resource" placeholder="Filter by resource code…"/></div>
              </th>

              <th data-sort="organization" onclick="sortBy('organization')">
                Organization
                <div class="hdr-ctrl"><input class="input" id="filter-org" placeholder="Filter org…"/></div>
              </th>

              <th>
                Errors
                <div class="small" data-lang="en">EN</div>
                <div class="small" data-lang="fr" style="display:none">FR</div>
              </th>

              <th data-sort="status" onclick="sortBy('status')">
                Status
                <div class="hdr-ctrl">
                  <select class="select" id="filter-status">
                    <option value="">All</option>
                    <option value="success">success</option>
                    <option value="failure">failure</option>
                  </select>
                </div>
              </th>

              <th data-sort="created" onclick="sortBy('created')">
                Created
                <div class="hdr-ctrl"><input class="input" id="filter-created" placeholder="yyyy-mm… or text"/></div>
              </th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>

      <div class="pager">
        <span class="subtle" id="pager-info"></span>
        <span class="subtle">Rows per page</span>
        <select class="select" id="page-size">
          <option value="25" selected>25</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="500">500</option>
        </select>
        <button class="btn" onclick="gotoPrev()">Prev</button>
        <button class="btn" onclick="gotoNext()">Next</button>
      </div>
    </div>

    <p class="subtle" style="margin-top:12px">
      v0.2 = tasks schema (priority). v0.1 = tables schema (legacy).
      Language toggle switches dataset & resource titles and which errors column is shown (EN/FR).
    </p>
  </div>
</body></html>"""

    with open(os.path.join(out_dir,"index.html"), "w", encoding="utf-8") as f:
        f.write(html_index)

def write_org_index(org_groups, out_dir):
    org_dir = os.path.join(out_dir, "organizations")
    os.makedirs(org_dir, exist_ok=True)

    items_html = []
    for group in org_groups:
        success = group["status_counts"].get("success", 0)
        failure = group["status_counts"].get("failure", 0)
        other = group["total"] - success - failure
        url_types_count = len(group["url_counts"])
        latest_created = group.get("latest_created") or "N/A"
        match = f"{group['name']} {success} {failure} {other} {url_types_count}".lower()
        items_html.append(f"""
      <li class="panel section" data-match="{html.escape(match)}">
        <div style="display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap">
          <div class="h1" style="font-size:18px">{html.escape(group['name'])}</div>
          <a class="btn" href="{html.escape(group['slug'])}.html">Open report</a>
        </div>
        <div class="summary-grid" style="margin-top:16px">
          <div class="summary-tile">
            <div class="label">Total reports</div>
            <div class="value">{group['total']}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Success</div>
            <div class="value" style="color:var(--good)">{success}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Failure</div>
            <div class="value" style="color:var(--bad)">{failure}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Other statuses</div>
            <div class="value">{max(other, 0)}</div>
          </div>
        </div>
        <p class="subtle" style="margin-top:12px">
          URL types: {url_types_count} · Latest report: <time>{html.escape(latest_created)}</time>
        </p>
      </li>
    """)

    html_page = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Organizations · Validation Reports</title>
<link rel="stylesheet" href="../style.css"><script defer src="../app.js"></script>
</head><body>
  <div class="container">
    <div class="header">
      <div class="h1"><a href="../index.html">← Validation Reports</a></div>
      <div class="actions">
        <div class="lang-toggle">
          <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
          <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="controls">
        <div class="grow">
          <input class="input" id="org-search" placeholder="Search organizations…"/>
        </div>
      </div>
      <ul class="org-list" id="org-list">
        {''.join(items_html) if items_html else '<li class="panel section"><span class="badge na">No organizations found</span></li>'}
      </ul>
    </div>
  </div>
  <script>
    document.addEventListener('DOMContentLoaded', function(){{
      const search = document.getElementById('org-search');
      const rows = Array.from(document.querySelectorAll('#org-list [data-match]'));
      if(search){{
        search.addEventListener('input', function(){{
          const q = search.value.trim().toLowerCase();
          rows.forEach(li => {{
            li.style.display = (!q || li.dataset.match.includes(q)) ? '' : 'none';
          }});
        }});
      }}
    }});
  </script>
</body></html>"""

    with open(os.path.join(org_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html_page)

def write_org_pages(org_groups, out_dir):
    org_dir = os.path.join(out_dir, "organizations")
    os.makedirs(org_dir, exist_ok=True)

    for group in org_groups:
        summary = prepare_org_summary(group)
        success = summary["success"]
        failure = summary["failure"]
        other = summary["other"]
        url_types_count = summary["url_types_count"]
        status_labels = summary["status_labels"]
        status_data_json = summary["status_data_json"]
        url_chart_json = summary["url_chart_json"]
        status_table_headers = "".join(f"<th>{html.escape(label)}</th>" for label in status_labels)
        status_table_rows = []
        for row in summary["status_table_rows"]:
            cells = "".join(f"<td>{value}</td>" for value in row["counts"])
            status_table_rows.append(f"<tr><td>{html.escape(row['label'])}</td><td>{row['total']}</td>{cells}</tr>")
        if not status_table_rows:
            status_table_rows.append('<tr><td colspan="99"><span class="badge na">No URL types</span></td></tr>')

        latest_created = summary["latest_created"]
        report_rows = [render_report_row(it, "../reports") for it in group["items"]]
        if not report_rows:
            report_rows.append('<tr><td colspan="7"><span class="badge na">No reports found</span></td></tr>')

        page_html = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(group['name'])} · Validation Reports</title>
<link rel="stylesheet" href="../style.css">
<script defer src="../app.js"></script>
<script src="{CHART_JS_URL}"></script>
</head><body>
  <div class="container">
    <div class="header">
      <div class="h1"><a href="index.html">← Organizations</a></div>
      <div class="actions">
        <a class="btn" href="../index.html">All reports</a>
        <div class="lang-toggle">
          <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
          <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
        </div>
      </div>
    </div>

    <div class="panel section">
      <div class="h1" style="font-size:20px">{html.escape(group['name'])}</div>
      <p class="subtle" style="margin-top:8px">
        Total reports: {group['total']} · Success: {success} · Failure: {failure} · Other: {max(other, 0)} · URL types: {url_types_count} · Latest: <time>{html.escape(latest_created)}</time>
      </p>
      <div class="summary-grid" style="margin-top:16px">
        <div class="summary-tile">
          <div class="label">Total reports</div>
          <div class="value">{group['total']}</div>
        </div>
        <div class="summary-tile">
          <div class="label">Success</div>
          <div class="value" style="color:var(--good)">{success}</div>
        </div>
        <div class="summary-tile">
          <div class="label">Failure</div>
          <div class="value" style="color:var(--bad)">{failure}</div>
        </div>
        <div class="summary-tile">
          <div class="label">URL types</div>
          <div class="value">{url_types_count}</div>
        </div>
      </div>
    </div>

    <div class="chart-grid">
      <div class="panel chart-card">
        <h3>Status distribution</h3>
        <canvas id="statusChart"></canvas>
      </div>
      <div class="panel chart-card">
        <h3>Reports by URL type &amp; status</h3>
        <canvas id="urlTypeChart"></canvas>
      </div>
    </div>

    <div class="panel section">
      <h3 style="margin:0 0 12px">URL type breakdown</h3>
      <div class="table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>URL type</th>
              <th>Total</th>
              {status_table_headers}
            </tr>
          </thead>
          <tbody>
            {''.join(status_table_rows)}
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <div class="controls">
        <div class="grow"><input class="input" id="q" placeholder="Search within this organization…"/></div>
        <div>
          <label class="subtle">Status<br/>
            <select class="select" id="filter-status">
              <option value="">All</option>
              <option value="success">success</option>
              <option value="failure">failure</option>
            </select>
          </label>
        </div>
        <div>
          <label class="subtle">Created<br/>
            <input class="input" id="filter-created" placeholder="yyyy-mm…"/>
          </label>
        </div>
        <div>
          <label class="subtle">Version<br/>
            <select class="select" id="filter-version">
              <option value="">All</option>
              <option value="v0.2">v0.2</option>
              <option value="v0.1">v0.1</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
        </div>
      </div>

      <div class="table-wrap">
        <table class="table" data-report-table>
          <thead>
            <tr>
              <th>ID</th>

              <th data-sort="dataset" onclick="sortBy('dataset')">
                Dataset<br/>
                <div class="small">Title (EN/FR)</div>
              </th>

              <th data-sort="resource" onclick="sortBy('resource')">
                Resource
                <div class="hdr-ctrl"><input class="input" id="filter-resource" placeholder="Filter by resource code…"/></div>
              </th>

              <th data-sort="organization" onclick="sortBy('organization')">
                Organization
              </th>

              <th>
                Errors
                <div class="small" data-lang="en">EN</div>
                <div class="small" data-lang="fr" style="display:none">FR</div>
              </th>

              <th data-sort="status" onclick="sortBy('status')">
                Status
              </th>

              <th data-sort="created" onclick="sortBy('created')">
                Created
              </th>
            </tr>
          </thead>
          <tbody>
            {''.join(report_rows)}
          </tbody>
        </table>
      </div>

      <div class="pager">
        <span class="subtle" id="pager-info"></span>
        <span class="subtle">Rows per page</span>
        <select class="select" id="page-size">
          <option value="25" selected>25</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="500">500</option>
        </select>
        <button class="btn" onclick="gotoPrev()">Prev</button>
        <button class="btn" onclick="gotoNext()">Next</button>
      </div>
    </div>
  </div>

  <script>
    document.addEventListener('DOMContentLoaded', function(){{
      const tickColor = '#e8ecfa';
      const gridColor = 'rgba(255,255,255,0.1)';
      const statusData = {status_data_json};
      const urlTypeData = {url_chart_json};
      if(window.Chart){{
        const statusCanvas = document.getElementById('statusChart');
        if(statusCanvas){{
          new Chart(statusCanvas.getContext('2d'), {{
            type: 'pie',
            data: statusData,
            options: {{
              plugins: {{
                legend: {{
                  position: 'bottom',
                  labels: {{ color: tickColor }}
                }}
              }}
            }}
          }});
        }}
        const urlCanvas = document.getElementById('urlTypeChart');
        if(urlCanvas){{
          new Chart(urlCanvas.getContext('2d'), {{
            type: 'bar',
            data: urlTypeData,
            options: {{
              plugins: {{
                legend: {{
                  labels: {{ color: tickColor }}
                }}
              }},
              responsive: true,
              scales: {{
                x: {{
                  stacked: true,
                  ticks: {{ color: tickColor }},
                  grid: {{ color: gridColor }}
                }},
                y: {{
                  stacked: true,
                  ticks: {{ color: tickColor }},
                  grid: {{ color: gridColor }},
                  beginAtZero: true
                }}
              }}
            }}
          }});
        }}
      }}
    }});
  </script>
</body></html>"""

        with open(os.path.join(org_dir, f"{group['slug']}.html"), "w", encoding="utf-8") as f:
            f.write(page_html)

# --------------------- Detail pages (versioned) ---------------------

def render_errors_table(errs, lang='en'):
    if not errs:
        return '<p class="badge ok">No errors</p>' if lang=='en' else '<p class="badge ok">Aucune erreur</p>'
    head = '<thead><tr><th>Row</th><th>Field</th><th>Code</th><th>Message</th></tr></thead>' if lang=='en' \
         else '<thead><tr><th>Ligne</th><th>Champ</th><th>Code</th><th>Message</th></tr></thead>'
    rows=''.join(
        f"<tr><td>{html.escape(str(e.get('rowNumber','')))}</td>"
        f"<td>{html.escape(str(e.get('fieldName','')))}</td>"
        f"<td>{html.escape(str(e.get('code','')))}</td>"
        f"<td>{html.escape(str(e.get('message','')))}</td></tr>"
        for e in errs[:1000]
    )
    return f'<table class="table">{head}<tbody>{rows}</tbody></table>'

# v0.2 tasks blocks
def render_task_block(task, lang='en', idx=0):
    st=task.get("stats") or {}
    labels=task.get("labels") or []
    warns =task.get("warnings") or []
    errs  =task.get("errors") or []
    name  =task.get("name") or f"Task {idx+1}"
    ttype =task.get("type") or ""
    place =task.get("place") or ""

    kv=[]
    kv.append(f"<div>Valid</div><div>{badge_state(task.get('valid'))}</div>")
    for label,key in [("Type","type"),("Source","place"),("Rows","rows"),("Fields","fields"),("Errors","errors"),
                      ("Warnings","warnings"),("Bytes","bytes"),("MD5","md5"),("SHA256","sha256"),("Seconds","seconds")]:
        val = st.get(key) if key in ("rows","fields","errors","warnings","bytes","md5","sha256","seconds") else task.get(key)
        if val not in (None,"",[]):
            if key=="place":
                val = f'<a href="{html.escape(str(val))}" target="_blank" rel="noopener">{html.escape(str(val))}</a>'
            kv.append(f"<div>{label}</div><div>{val}</div>")

    labels_html="<br/>".join(html.escape(str(x)) for x in labels) if labels else '<span class="subtle">(none)</span>'
    warns_html ="<br/>".join(html.escape(str(x)) for x in warns)  if warns  else '<span class="subtle">(none)</span>'
    errs_html  ="<br/>".join(html.escape(str(x)) for x in errs)   if errs   else '<span class="subtle">(none)</span>'
    raw_json   = html.escape(json.dumps(task, ensure_ascii=False, indent=2))

    return f"""
    <div class="section">
      <div class="h1" style="font-size:16px">{html.escape(name)} {chip(ttype, 'na')}</div>
      <div class="kv" style="margin-top:8px">{''.join(kv)}</div>
      <h4 style="margin:12px 0 6px">{'Labels' if lang=='en' else 'Étiquettes'}</h4><div class="code">{labels_html}</div>
      <h4 style="margin:12px 0 6px">{'Warnings' if lang=='en' else 'Avertissements'}</h4><div class="code">{warns_html}</div>
      <h4 style="margin:12px 0 6px">{'Errors' if lang=='en' else 'Erreurs'}</h4><div class="code">{errs_html}</div>
      <h4 style="margin:12px 0 6px">{'Raw task JSON' if lang=='en' else 'JSON brut de la tâche'}</h4><div class="code">{raw_json}</div>
    </div>"""

def render_lang_panel_v02(lang, tasks):
    a=agg_v02(tasks) if tasks is not None else {"valid_all":None,"error_count":0,"warning_count":0,"row_count":0}
    head=f"""
      <div class="section">
        <div class="kv">
          <div>{"Valid" if lang=='en' else "Valide"}</div><div>{badge_state(a["valid_all"])}</div>
          <div>{"Errors" if lang=='en' else "Erreurs"}</div><div>{a["error_count"]}</div>
          <div>{"Warnings" if lang=='en' else "Avertissements"}</div><div>{a["warning_count"]}</div>
          <div>{"Rows" if lang=='en' else "Lignes"}</div><div>{a["row_count"]}</div>
        </div>
      </div>"""
    blocks="".join(render_task_block(t,lang,i) for i,t in enumerate(tasks or [])) or \
        ('<div class="section"><span class="badge na">No tasks</span></div>' if lang=='en' else '<div class="section"><span class="badge na">Aucune tâche</span></div>')
    style='' if lang=='en' else 'style="display:none"'
    return f'<section data-lang="{lang}" {style} class="panel">{head}{blocks}</section>'

# v0.1 tables blocks
def render_table_block_v01(t, lang='en', idx=0):
    headers=t.get("headers",[])
    header_text=html.escape("\n".join(map(str,headers))) if headers else '<span class="subtle">(none)</span>'
    kv=[]
    for label,key in [("Valid","valid"),("Format","format"),("Encoding","encoding"),("Scheme","scheme"),
                      ("Source","source"),("Time","time"),("Row count","row-count"),("Row count","row_count"),
                      ("Error count","error-count"),("Error count","error_count")]:
        val=t.get(key)
        if label in ("Row count","Error count") and val is None:
            alt = key.replace("-","_") if "-" in key else key.replace("_","-")
            val=t.get(alt)
        if val not in (None,"",[]):
            if key=="valid": val=badge_state(bool(val))
            kv.append(f"<div>{label}</div><div>{html.escape(str(val))}</div>")
    errors_table=render_errors_table(t.get("errors",[]),lang)
    raw_json=html.escape(json.dumps(t, ensure_ascii=False, indent=2))
    return f"""
      <div class="section">
        <div class="h1" style="font-size:16px">Table {idx+1}</div>
        <div class="kv" style="margin-top:8px">{''.join(kv) or '<div>Table</div><div>-</div>'}</div>
        <h4 style="margin:12px 0 6px">{'Errors' if lang=='en' else 'Erreurs'}</h4>{errors_table}
        <h4 style="margin:12px 0 6px">{'Headers' if lang=='en' else 'En-têtes'}</h4><div class="code">{header_text}</div>
        <h4 style="margin:12px 0 6px">{'Raw table JSON' if lang=='en' else 'JSON brut de la table'}</h4><div class="code">{raw_json}</div>
      </div>"""

def render_lang_panel_v01(lang, tables):
    a=agg_v01(tables) if tables is not None else {"valid_all":None,"error_count":0,"row_count":0}
    head=f"""
      <div class="section">
        <div class="kv">
          <div>{"Valid" if lang=='en' else "Valide"}</div><div>{badge_state(a["valid_all"])}</div>
          <div>{"Errors" if lang=='en' else "Erreurs"}</div><div>{a["error_count"]}</div>
          <div>{"Rows" if lang=='en' else "Lignes"}</div><div>{a["row_count"]}</div>
        </div>
      </div>"""
    blocks="".join(render_table_block_v01(t,lang,i) for i,t in enumerate(tables or [])) or \
        ('<div class="section"><span class="badge na">No tables</span></div>' if lang=='en' else '<div class="section"><span class="badge na">Aucune table</span></div>')
    style='' if lang=='en' else 'style="display:none"'
    return f'<section data-lang="{lang}" {style} class="panel">{head}{blocks}</section>'

def write_report_pages(items, out_dir, org_lookup=None):
    rdir=os.path.join(out_dir,"reports"); os.makedirs(rdir, exist_ok=True)
    for it in items:
        pid=slugify(it['id']); ver=it.get("version") or "unknown"

        # Dataset links (edit + portal)
        dsid = it.get('dataset_id','')
        edit_url   = f"https://registry.open.canada.ca/dataset/{html.escape(dsid)}"
        portal_url = f"https://open.canada.ca/data/en/dataset/{html.escape(dsid)}"
        dataset_links = f'''
          <div>
            <a class="badge link" href="{edit_url}" target="_blank" rel="noopener">edit</a>
            &nbsp;
            <a class="badge link" href="{portal_url}" target="_blank" rel="noopener">portal</a>
          </div>
        '''

        org_name = normalize_org_name(it.get('organization_name'))
        org_slug = (org_lookup or {}).get(org_name)
        if org_slug:
            org_cell = f'<a href="../organizations/{html.escape(org_slug)}.html">{html.escape(org_name)}</a>'
        else:
            org_cell = html.escape(org_name)

        header_meta=f"""
        <div class="kv" style="margin-top:8px">
          <div>Version</div><div>{chip(ver,'na')}</div>
          <div>Organization</div><div>{org_cell}</div>
          <div>Dataset</div><div>{lang_html(it.get('dataset_title_en',''), it.get('dataset_title_fr',''))} <span class="small"><code>{html.escape(dsid)}</code></span>{dataset_links}</div>
          <div>Resource</div><div>{lang_html(it.get('resource_name_en',''), it.get('resource_name_fr',''))} <span class="small"><code>{html.escape(it.get('resource_id',''))}</code> · {html.escape(it.get('url_type',''))}</span></div>
          <div>Status</div><div>{chip(it['status'] or 'unknown', 'na' if it['status'] not in ('success','failure') else ('ok' if it['status']=='success' else 'bad'))}</div>
          <div>Created</div><div><time>{html.escape(it['created'] or '')}</time></div>
        </div>"""

        if ver=="v0.2":
            body=f"{render_lang_panel_v02('en', it['lang_data'].get('en'))}{render_lang_panel_v02('fr', it['lang_data'].get('fr'))}"
        elif ver=="v0.1":
            body=f"{render_lang_panel_v01('en', it['lang_data'].get('en'))}{render_lang_panel_v01('fr', it['lang_data'].get('fr'))}"
        else:
            body='<div class="panel section"><span class="badge na">Unknown report format</span></div>'

        page=f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Report {html.escape(it['id'])}</title>
<link rel="stylesheet" href="../style.css"><script defer src="../app.js"></script>
</head><body>
  <div class="container">
    <div class="header">
      <div class="h1"><a href="../index.html">← Reports</a></div>
      <div class="lang-toggle">
        <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
        <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
      </div>
    </div>

    <div class="panel section">
      <div class="h1" style="font-size:18px"><code>{html.escape(it['id'])}</code></div>
      {header_meta}
    </div>

    {body}
  </div>
</body></html>"""
        with open(os.path.join(rdir, f"{pid}.html"), "w", encoding="utf-8") as f:
            f.write(page)




############################################################
# Improved GCDS Government of Canada Design System PAGES
############################################################

GCDS_CSS_SHORTCUTS = "https://cdn.design-system.alpha.canada.ca/@gcds-core/css-shortcuts@1.0.0/dist/gcds-css-shortcuts.min.css"
GCDS_COMPONENTS_CSS = "https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.css"
GCDS_COMPONENTS_JS  = "https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.43.1/dist/gcds/gcds.esm.js"

GC_CSS = """
:root{
  --gc-surface:#ffffff;
  --gc-surface-alt:#f3f6f9;
  --gc-border:#dfe3e8;
  --gc-text:#1b1b1b;
  --gc-muted:#5a6b7d;
  --gc-link:#1a5a96;
  --gc-good:#2e7d32;
  --gc-bad:#c62828;
}
body{
  margin:0;
  background:var(--gc-surface-alt);
  color:var(--gc-text);
  font-family:"Noto Sans","Helvetica Neue",Arial,sans-serif;
}
a{color:var(--gc-link);}
a:hover{text-decoration:underline;}
.gc-wrapper{padding:32px 0;}
.panel{background:var(--gc-surface); border:1px solid var(--gc-border); border-radius:16px; box-shadow:0 6px 24px rgba(0,0,0,0.05); margin-bottom:24px;}
.section{padding:24px;}
.controls{display:flex; flex-wrap:wrap; gap:16px; padding:20px 24px; border-bottom:1px solid var(--gc-border); background:#fbfcfd;}
.controls .grow{flex:1 1 240px;}
.input, .select, .btn{
  height:42px; padding:0 12px; border:1px solid var(--gc-border); border-radius:8px;
  background:#fff; color:var(--gc-text); font-size:15px; font-family:inherit;
  box-shadow:none;
}
.input:focus, .select:focus{outline:2px solid #1a5a96; outline-offset:2px;}
.btn{cursor:pointer; background:#1a5a96; color:#fff; border:none; font-weight:600;}
.btn:hover{background:#164d7f;}
.btn.secondary{background:#fff; color:#1a5a96; border:1px solid #1a5a96;}
.table{width:100%; border-collapse:collapse; font-size:15px;}
.table thead{background:#eef3f8;}
.table th, .table td{padding:12px 14px; border-bottom:1px solid var(--gc-border); vertical-align:top; text-align:left;}
.table th{position:sticky; top:0; z-index:1;}
.table tbody tr:nth-child(even){background:#fbfcfe;}
.hdr-ctrl{margin-top:8px;}
.hdr-ctrl .input{width:100%;}
.pager{display:flex; align-items:center; justify-content:flex-end; gap:12px; padding:16px 24px; border-top:1px solid var(--gc-border); background:#fbfcfd; flex-wrap:wrap;}
.table-wrap{max-height:70vh; overflow:auto;}
.badge{display:inline-flex; align-items:center; gap:6px; padding:3px 10px; border-radius:999px; background:#eef3f8; color:var(--gc-text); font-weight:600; font-size:13px;}
.badge.ok{background:#e3f2e6; color:var(--gc-good);}
.badge.bad{background:#fdecea; color:var(--gc-bad);}
.badge.na{background:#f1f2f6; color:var(--gc-muted);}
.badge.link{background:#fff; border:1px solid var(--gc-border);}
.small{font-size:13px; color:var(--gc-muted);}
.subtle{color:var(--gc-muted); font-size:13px;}
.lang-toggle{display:inline-flex; gap:8px;}
.lang-toggle .btn{height:36px; min-width:44px; background:#fff; color:#1a5a96; border:1px solid #1a5a96;}
.lang-toggle .btn.active{background:#1a5a96; color:#fff;}
.summary-grid{display:grid; gap:16px; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); margin:24px 0;}
.summary-tile{padding:20px; border-radius:12px; background:#fff; border:1px solid var(--gc-border);}
.summary-tile .label{font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--gc-muted);}
.summary-tile .value{font-size:22px; font-weight:700; margin-top:4px;}
.chart-grid{display:grid; gap:20px; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); margin:24px 0;}
.chart-card{padding:24px; border-radius:12px; border:1px solid var(--gc-border); background:#fff;}
.chart-card h3{margin:0 0 16px; font-size:18px;}
.chart-card canvas{width:100%; height:320px;}
.org-list{list-style:none; padding:0; margin:0; display:grid; gap:20px;}
.actions{display:flex; align-items:center; gap:12px; flex-wrap:wrap;}
.h1{font-size:24px; font-weight:700; margin:0;}
.kv{display:grid; grid-template-columns:200px 1fr; gap:10px; font-size:15px;}
.badge.link{display:inline-flex;}
.panel.section .h1 code{font-size:18px;}
@media (max-width: 600px){
  .controls{flex-direction:column;}
  .kv{grid-template-columns:1fr;}
}
"""
def write_gcds_index(items, org_groups, out_dir):
    """Write the improved GCDS main index page using latest GCDS styles/components with feature parity."""

    today = datetime.utcnow().strftime("%Y-%m-%d")
    rows = [render_report_row(it, "gc_reports") for it in items]
    total_reports = len(items)
    success_count = sum(1 for it in items if normalize_status(it.get("status")) == "success")
    failure_count = sum(1 for it in items if normalize_status(it.get("status")) == "failure")
    other_count = total_reports - success_count - failure_count
    org_count = len(org_groups)

    html_code = f"""<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="description" content="Validation reports presented with the Government of Canada Design System." />
  <title>Validation Portal – GCDS</title>
  <link rel="stylesheet" href="{GCDS_CSS_SHORTCUTS}" />
  <link rel="stylesheet" href="{GCDS_COMPONENTS_CSS}" />
  <link rel="stylesheet" href="gc_style.css" />
  <script type="module" src="{GCDS_COMPONENTS_JS}"></script>
  <script defer src="app.js"></script>
</head>
<body>
  <gcds-header service-title="Validation Portal" service-href="gc_index.html" skip-to-href="#main-content"></gcds-header>
  <div class="gc-wrapper">
    <gcds-container id="main-content" main-container size="xl" centered tag="main">
      <section class="panel section">
        <div class="h1">Validation Portal (GCDS Theme)</div>
        <p class="subtle" style="margin:12px 0">
          This theme mirrors the Government of Canada layout while keeping all validation features.
        </p>
        <div class="actions">
          <a class="btn secondary" href="index.html">Primary theme</a>
          <a class="btn secondary" href="gc_organizations/index.html">Organizations</a>
          <div class="lang-toggle">
            <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
            <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
          </div>
        </div>
        <div class="summary-grid">
          <div class="summary-tile">
            <div class="label">Total reports</div>
            <div class="value">{total_reports}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Success</div>
            <div class="value" style="color:var(--gc-good)">{success_count}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Failure</div>
            <div class="value" style="color:var(--gc-bad)">{failure_count}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Other statuses</div>
            <div class="value">{other_count}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Organizations</div>
            <div class="value">{org_count}</div>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="controls">
          <div class="grow"><input class="input" id="q" placeholder="Search all columns…" /></div>
          <div>
            <label class="subtle">Version<br/>
              <select class="select" id="filter-version">
                <option value="">All</option>
                <option value="v0.2">v0.2</option>
                <option value="v0.1">v0.1</option>
                <option value="unknown">unknown</option>
              </select>
            </label>
          </div>
          <div>
            <label class="subtle">Organization<br/>
              <input class="input" id="filter-org" placeholder="Filter org…"/>
            </label>
          </div>
        </div>

        <div class="table-wrap">
          <table class="table" data-report-table>
            <thead>
              <tr>
                <th>ID</th>

                <th data-sort="dataset" onclick="sortBy('dataset')">
                  Dataset<br/>
                  <div class="small">Title (EN/FR)</div>
                </th>

                <th data-sort="resource" onclick="sortBy('resource')">
                  Resource
                  <div class="hdr-ctrl"><input class="input" id="filter-resource" placeholder="Filter by resource code…"/></div>
                </th>

                <th data-sort="organization" onclick="sortBy('organization')">
                  Organization
                </th>

                <th>
                  Errors
                  <div class="small" data-lang="en">EN</div>
                  <div class="small" data-lang="fr" style="display:none">FR</div>
                </th>

                <th data-sort="status" onclick="sortBy('status')">
                  Status
                  <div class="hdr-ctrl">
                    <select class="select" id="filter-status">
                      <option value="">All</option>
                      <option value="success">success</option>
                      <option value="failure">failure</option>
                    </select>
                  </div>
                </th>

                <th data-sort="created" onclick="sortBy('created')">
                  Created
                  <div class="hdr-ctrl"><input class="input" id="filter-created" placeholder="yyyy-mm…"/></div>
                </th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows)}
            </tbody>
          </table>
        </div>

        <div class="pager">
          <span class="subtle" id="pager-info"></span>
          <span class="subtle">Rows per page</span>
          <select class="select" id="page-size">
            <option value="25" selected>25</option>
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="500">500</option>
          </select>
          <button class="btn secondary" onclick="gotoPrev()">Prev</button>
          <button class="btn" onclick="gotoNext()">Next</button>
        </div>
      </section>

      <p class="subtle">
        Language toggle switches dataset and resource titles and keeps parity with the primary theme.
      </p>
      <gcds-date-modified>{today}</gcds-date-modified>
    </gcds-container>
  </div>
  <gcds-footer display="full" contextual-heading="Canadian Digital Service"></gcds-footer>
</body>
</html>
"""
    with open(os.path.join(out_dir, "gc_index.html"), "w", encoding="utf-8") as f:
        f.write(html_code)

def write_gcds_report_pages(items, out_dir, org_lookup=None):
    gc_reports_dir = os.path.join(out_dir, "gc_reports")
    os.makedirs(gc_reports_dir, exist_ok=True)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for it in items:
        pid = slugify(it['id'])
        ver = it.get("version") or "unknown"
        dsid = it.get('dataset_id','')
        edit_url   = f"https://registry.open.canada.ca/dataset/{html.escape(dsid)}"
        portal_url = f"https://open.canada.ca/data/en/dataset/{html.escape(dsid)}"
        dataset_links = f'''
          <div>
            <a class="badge link" href="{edit_url}" target="_blank" rel="noopener">edit</a>
            &nbsp;
            <a class="badge link" href="{portal_url}" target="_blank" rel="noopener">portal</a>
          </div>
        '''

        org_name = normalize_org_name(it.get('organization_name'))
        org_slug = (org_lookup or {}).get(org_name)
        if org_slug:
            org_cell = f'<a href="../gc_organizations/{html.escape(org_slug)}.html">{html.escape(org_name)}</a>'
        else:
            org_cell = html.escape(org_name)

        header_meta=f"""
        <div class="kv" style="margin-top:12px">
          <div>Version</div><div>{chip(ver,'na')}</div>
          <div>Organization</div><div>{org_cell}</div>
          <div>Dataset</div><div>{lang_html(it.get('dataset_title_en',''), it.get('dataset_title_fr',''))} <span class="small"><code>{html.escape(dsid)}</code></span>{dataset_links}</div>
          <div>Resource</div><div>{lang_html(it.get('resource_name_en',''), it.get('resource_name_fr',''))} <span class="small"><code>{html.escape(it.get('resource_id',''))}</code> · {html.escape(it.get('url_type',''))}</span></div>
          <div>Status</div><div>{chip(it['status'] or 'unknown', 'na' if it['status'] not in ('success','failure') else ('ok' if it['status']=='success' else 'bad'))}</div>
          <div>Created</div><div><time>{html.escape(it.get('created') or '')}</time></div>
        </div>"""

        if ver=="v0.2":
            body=f"{render_lang_panel_v02('en', it['lang_data'].get('en'))}{render_lang_panel_v02('fr', it['lang_data'].get('fr'))}"
        elif ver=="v0.1":
            body=f"{render_lang_panel_v01('en', it['lang_data'].get('en'))}{render_lang_panel_v01('fr', it['lang_data'].get('fr'))}"
        else:
            body='<div class="panel section"><span class="badge na">Unknown report format</span></div>'

        report_html = f"""<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="description" content="Validation Report for {html.escape(it['id'])}" />
  <title>Validation Report {html.escape(it['id'])} – GCDS</title>
  <link rel="stylesheet" href="{GCDS_CSS_SHORTCUTS}" />
  <link rel="stylesheet" href="{GCDS_COMPONENTS_CSS}" />
  <link rel="stylesheet" href="../gc_style.css" />
  <script type="module" src="{GCDS_COMPONENTS_JS}"></script>
  <script defer src="../app.js"></script>
</head>
<body>
  <gcds-header service-title="Validation Portal" service-href="../gc_index.html" skip-to-href="#main-content"></gcds-header>
  <div class="gc-wrapper">
    <gcds-container id="main-content" main-container size="xl" centered tag="main">
      <section class="panel section">
        <div class="actions" style="justify-content:space-between">
          <div class="h1"><code>{html.escape(it['id'])}</code></div>
          <div class="actions">
            <a class="btn secondary" href="../gc_index.html">Back to index</a>
            <a class="btn secondary" href="../reports/{html.escape(pid)}.html">Primary view</a>
            <div class="lang-toggle">
              <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
              <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
            </div>
          </div>
        </div>
        {header_meta}
      </section>

      {body}

      <gcds-date-modified>{today}</gcds-date-modified>
    </gcds-container>
  </div>
  <gcds-footer display="full" contextual-heading="Canadian Digital Service"></gcds-footer>
</body>
</html>
"""
        with open(os.path.join(gc_reports_dir, f"{pid}.html"), "w", encoding="utf-8") as f:
            f.write(report_html)

def write_gcds_org_index(org_groups, out_dir):
    org_dir = os.path.join(out_dir, "gc_organizations")
    os.makedirs(org_dir, exist_ok=True)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    items_html = []
    for group in org_groups:
        summary = prepare_org_summary(group)
        match = f"{group['name']} {summary['success']} {summary['failure']} {summary['other']} {summary['url_types_count']}".lower()
        items_html.append(f"""
      <li class="panel section" data-match="{html.escape(match)}">
        <div class="actions" style="justify-content:space-between">
          <div class="h1" style="font-size:20px">{html.escape(group['name'])}</div>
          <a class="btn secondary" href="{html.escape(group['slug'])}.html">Open report</a>
        </div>
        <div class="summary-grid">
          <div class="summary-tile">
            <div class="label">Total reports</div>
            <div class="value">{group['total']}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Success</div>
            <div class="value" style="color:var(--gc-good)">{summary['success']}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Failure</div>
            <div class="value" style="color:var(--gc-bad)">{summary['failure']}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Other statuses</div>
            <div class="value">{max(summary['other'], 0)}</div>
          </div>
        </div>
        <p class="subtle" style="margin-top:12px">
          URL types: {summary['url_types_count']} · Latest report: <time>{html.escape(summary['latest_created'])}</time>
        </p>
      </li>
    """)

    html_page = f"""<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="description" content="Validation organizations (GCDS theme)." />
  <title>Organizations · Validation Portal (GCDS)</title>
  <link rel="stylesheet" href="{GCDS_CSS_SHORTCUTS}" />
  <link rel="stylesheet" href="{GCDS_COMPONENTS_CSS}" />
  <link rel="stylesheet" href="../gc_style.css" />
  <script type="module" src="{GCDS_COMPONENTS_JS}"></script>
  <script defer src="../app.js"></script>
</head>
<body>
  <gcds-header service-title="Validation Portal" service-href="../gc_index.html" skip-to-href="#main-content"></gcds-header>
  <div class="gc-wrapper">
    <gcds-container id="main-content" main-container size="xl" centered tag="main">
      <section class="panel section">
        <div class="actions" style="justify-content:space-between">
          <div class="h1">Organizations</div>
          <div class="actions">
            <a class="btn secondary" href="../gc_index.html">Back to index</a>
            <a class="btn secondary" href="../organizations/index.html">Primary theme</a>
            <div class="lang-toggle">
              <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
              <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
            </div>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="controls">
          <div class="grow">
            <input class="input" id="org-search" placeholder="Search organizations…"/>
          </div>
        </div>
        <ul class="org-list" id="org-list">
          {''.join(items_html) if items_html else '<li class="panel section"><span class="badge na">No organizations found</span></li>'}
        </ul>
      </section>

      <gcds-date-modified>{today}</gcds-date-modified>
    </gcds-container>
  </div>
  <gcds-footer display="full" contextual-heading="Canadian Digital Service"></gcds-footer>
  <script>
    document.addEventListener('DOMContentLoaded', function(){{
      const search = document.getElementById('org-search');
      const items = Array.from(document.querySelectorAll('#org-list [data-match]'));
      if(search){{
        search.addEventListener('input', function(){{
          const q = search.value.trim().toLowerCase();
          items.forEach(item => {{
            item.style.display = (!q || item.dataset.match.includes(q)) ? '' : 'none';
          }});
        }});
      }}
    }});
  </script>
</body>
</html>
"""

    with open(os.path.join(org_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html_page)

def write_gcds_org_pages(org_groups, out_dir):
    org_dir = os.path.join(out_dir, "gc_organizations")
    os.makedirs(org_dir, exist_ok=True)

    for group in org_groups:
        summary = prepare_org_summary(group)
        status_table_headers = "".join(f"<th>{html.escape(label)}</th>" for label in summary["status_labels"])
        status_table_rows = []
        for row in summary["status_table_rows"]:
            cells = "".join(f"<td>{value}</td>" for value in row["counts"])
            status_table_rows.append(f"<tr><td>{html.escape(row['label'])}</td><td>{row['total']}</td>{cells}</tr>")
        if not status_table_rows:
            status_table_rows.append('<tr><td colspan="99"><span class="badge na">No URL types</span></td></tr>')

        report_rows = [render_report_row(it, "../gc_reports") for it in group["items"]]
        if not report_rows:
            report_rows.append('<tr><td colspan="7"><span class="badge na">No reports found</span></td></tr>')

        success = summary["success"]
        failure = summary["failure"]
        other = summary["other"]
        url_types_count = summary["url_types_count"]
        latest_created = summary["latest_created"]

        page_html = f"""<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="description" content="Organization validation reports for {html.escape(group['name'])} (GCDS theme)." />
  <title>{html.escape(group['name'])} · GCDS Validation Reports</title>
  <link rel="stylesheet" href="{GCDS_CSS_SHORTCUTS}" />
  <link rel="stylesheet" href="{GCDS_COMPONENTS_CSS}" />
  <link rel="stylesheet" href="../gc_style.css" />
  <script type="module" src="{GCDS_COMPONENTS_JS}"></script>
  <script src="{CHART_JS_URL}"></script>
  <script defer src="../app.js"></script>
</head>
<body>
  <gcds-header service-title="Validation Portal" service-href="../gc_index.html" skip-to-href="#main-content"></gcds-header>
  <div class="gc-wrapper">
    <gcds-container id="main-content" main-container size="xl" centered tag="main">
      <section class="panel section">
        <div class="actions" style="justify-content:space-between">
          <div class="h1">{html.escape(group['name'])}</div>
          <div class="actions">
            <a class="btn secondary" href="../gc_index.html">All reports</a>
            <a class="btn secondary" href="index.html">Organizations</a>
            <a class="btn secondary" href="../organizations/{html.escape(group['slug'])}.html">Primary view</a>
            <div class="lang-toggle">
              <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
              <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
            </div>
          </div>
        </div>
        <p class="subtle" style="margin-top:12px">
          Total reports: {group['total']} · Success: {success} · Failure: {failure} · Other: {max(other, 0)} · URL types: {url_types_count} · Latest: <time>{html.escape(latest_created)}</time>
        </p>
        <div class="summary-grid">
          <div class="summary-tile">
            <div class="label">Total reports</div>
            <div class="value">{group['total']}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Success</div>
            <div class="value" style="color:var(--gc-good)">{success}</div>
          </div>
          <div class="summary-tile">
            <div class="label">Failure</div>
            <div class="value" style="color:var(--gc-bad)">{failure}</div>
          </div>
          <div class="summary-tile">
            <div class="label">URL types</div>
            <div class="value">{url_types_count}</div>
          </div>
        </div>
      </section>

      <div class="chart-grid">
        <div class="chart-card">
          <h3>Status distribution</h3>
          <canvas id="statusChart"></canvas>
        </div>
        <div class="chart-card">
          <h3>Reports by URL type &amp; status</h3>
          <canvas id="urlTypeChart"></canvas>
        </div>
      </div>

      <section class="panel section">
        <h3 style="margin:0 0 16px">URL type breakdown</h3>
        <div class="table-wrap">
          <table class="table">
            <thead>
              <tr>
                <th>URL type</th>
                <th>Total</th>
                {status_table_headers}
              </tr>
            </thead>
            <tbody>
              {''.join(status_table_rows)}
            </tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <div class="controls">
          <div class="grow"><input class="input" id="q" placeholder="Search within this organization…"/></div>
          <div>
            <label class="subtle">Status<br/>
              <select class="select" id="filter-status">
                <option value="">All</option>
                <option value="success">success</option>
                <option value="failure">failure</option>
              </select>
            </label>
          </div>
          <div>
            <label class="subtle">Created<br/>
              <input class="input" id="filter-created" placeholder="yyyy-mm…"/>
            </label>
          </div>
          <div>
            <label class="subtle">Version<br/>
              <select class="select" id="filter-version">
                <option value="">All</option>
                <option value="v0.2">v0.2</option>
                <option value="v0.1">v0.1</option>
                <option value="unknown">unknown</option>
              </select>
            </label>
          </div>
        </div>

        <div class="table-wrap">
          <table class="table" data-report-table>
            <thead>
              <tr>
                <th>ID</th>

                <th data-sort="dataset" onclick="sortBy('dataset')">
                  Dataset<br/>
                  <div class="small">Title (EN/FR)</div>
                </th>

                <th data-sort="resource" onclick="sortBy('resource')">
                  Resource
                  <div class="hdr-ctrl"><input class="input" id="filter-resource" placeholder="Filter by resource code…"/></div>
                </th>

                <th data-sort="organization" onclick="sortBy('organization')">
                  Organization
                </th>

                <th>
                  Errors
                  <div class="small" data-lang="en">EN</div>
                  <div class="small" data-lang="fr" style="display:none">FR</div>
                </th>

                <th data-sort="status" onclick="sortBy('status')">
                  Status
                </th>

                <th data-sort="created" onclick="sortBy('created')">
                  Created
                </th>
              </tr>
            </thead>
            <tbody>
              {''.join(report_rows)}
            </tbody>
          </table>
        </div>

        <div class="pager">
          <span class="subtle" id="pager-info"></span>
          <span class="subtle">Rows per page</span>
          <select class="select" id="page-size">
            <option value="25" selected>25</option>
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="500">500</option>
          </select>
          <button class="btn secondary" onclick="gotoPrev()">Prev</button>
          <button class="btn" onclick="gotoNext()">Next</button>
        </div>
      </section>

      <gcds-date-modified>{datetime.utcnow().strftime("%Y-%m-%d")}</gcds-date-modified>
    </gcds-container>
  </div>
  <gcds-footer display="full" contextual-heading="Canadian Digital Service"></gcds-footer>
  <script>
    document.addEventListener('DOMContentLoaded', function(){{
      const tickColor = '#1b1b1b';
      const gridColor = 'rgba(0,0,0,0.08)';
      const statusData = {summary["status_data_json"]};
      const urlTypeData = {summary["url_chart_json"]};
      if(window.Chart){{
        const statusCanvas = document.getElementById('statusChart');
        if(statusCanvas){{
          new Chart(statusCanvas.getContext('2d'), {{
            type: 'pie',
            data: statusData,
            options: {{
              plugins: {{
                legend: {{
                  position: 'bottom',
                  labels: {{ color: tickColor }}
                }}
              }}
            }}
          }});
        }}
        const urlCanvas = document.getElementById('urlTypeChart');
        if(urlCanvas){{
          new Chart(urlCanvas.getContext('2d'), {{
            type: 'bar',
            data: urlTypeData,
            options: {{
              plugins: {{
                legend: {{
                  labels: {{ color: tickColor }}
                }}
              }},
              responsive: true,
              scales: {{
                x: {{
                  stacked: true,
                  ticks: {{ color: tickColor }},
                  grid: {{ color: gridColor }}
                }},
                y: {{
                  stacked: true,
                  ticks: {{ color: tickColor }},
                  grid: {{ color: gridColor }},
                  beginAtZero: true
                }}
              }}
            }}
          }});
        }}
      }}
    }});
  </script>
</body>
</html>
"""
        with open(os.path.join(org_dir, f"{group['slug']}.html"), "w", encoding="utf-8") as f:
            f.write(page_html)

############################################################
# Main
############################################################

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    theme = SITE_THEME or "both"
    build_primary = theme in ("both", "primary", "")
    build_gcds = theme in ("both", "gcds")
    if not build_primary and not build_gcds:
        build_primary = True

    with open(os.path.join(OUT_DIR, "style.css"), "w", encoding="utf-8") as f:
        f.write(CSS)
    with open(os.path.join(OUT_DIR, "app.js"), "w", encoding="utf-8") as f:
        f.write(JS)
    if build_gcds:
        with open(os.path.join(OUT_DIR, "gc_style.css"), "w", encoding="utf-8") as f:
            f.write(GC_CSS)

    items=read_items(IN_PATH)
    org_groups = build_org_groups(items)
    org_lookup = {group["name"]: group["slug"] for group in org_groups}

    themes_rendered = []

    if build_primary:
        write_index(items, OUT_DIR)
        write_org_index(org_groups, OUT_DIR)
        write_report_pages(items, OUT_DIR, org_lookup)
        write_org_pages(org_groups, OUT_DIR)
        themes_rendered.append("primary")

    if build_gcds:
        write_gcds_index(items, org_groups, OUT_DIR)
        write_gcds_report_pages(items, OUT_DIR, org_lookup)
        write_gcds_org_index(org_groups, OUT_DIR)
        write_gcds_org_pages(org_groups, OUT_DIR)
        themes_rendered.append("gcds")

    theme_label = ", ".join(themes_rendered) if themes_rendered else "none"
    print(f"✓ Site built ({theme_label}): {OUT_DIR}/  reports: {len(items)}")

if __name__ == "__main__":
    main()
