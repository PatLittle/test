#!/usr/bin/env python3
"""
Static site generator for CKAN validation reports with explicit versioning:

- v0.2 = language buckets holding `tasks: [...]` (new, preferred)
- v0.1 = language buckets holding `tables: [...]` (older)

We render each version with a purpose-built template (no compromises).
The index supports search, filters, sorting, and pagination.

ENV:
  VALIDATION_JSONL (default: validation.jsonl)
  SITE_DIR         (default: VALIDATION)
"""

import os, re, json, html, ujson

IN_PATH  = os.getenv("VALIDATION_JSONL", "validation.jsonl")
OUT_DIR  = os.getenv("SITE_DIR", "VALIDATION")

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

def parse_created_era(s):
    import datetime as dt
    cutoff = dt.datetime(2024, 12, 10)
    if not s:
        return "unknown"
    fmts = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f")
    for fmt in fmts:
        try:
            d = dt.datetime.strptime(s[:len(fmt)], fmt)
            return "new" if d >= cutoff else "old"
        except Exception:
            continue
    return "unknown"

# --------------------- Version detection ---------------------

def detect_version(rep_dict):
    """
    Return 'v0.2' if any language or generic holder contains `tasks: [...]`.
    Return 'v0.1' if any contains `tables: [...]`.
    Otherwise 'unknown'.
    """
    if not isinstance(rep_dict, dict):
        return "unknown"

    # Check language buckets first
    for lang in ("en", "fr"):
        v = rep_dict.get(lang)
        if isinstance(v, dict):
            if isinstance(v.get("tasks"), list):
                return "v0.2"
            if isinstance(v.get("tables"), list):
                return "v0.1"

    # Generic holders (fallback if no en/fr)
    for holder in ("report", "data"):
        v = rep_dict.get(holder)
        if isinstance(v, dict):
            if isinstance(v.get("tasks"), list):
                return "v0.2"
            if isinstance(v.get("tables"), list):
                return "v0.1"

    # Very old/odd cases with top-level
    if isinstance(rep_dict.get("tasks"), list):
        return "v0.2"
    if isinstance(rep_dict.get("tables"), list):
        return "v0.1"

    return "unknown"

def lang_block(rep_dict, lang_or_holder):
    v = rep_dict.get(lang_or_holder)
    return v if isinstance(v, dict) else {}

def extract_lang_v02(rep_dict):
    """
    Return tasks by language: {'en':[...], 'fr':[...]}
    Falls back to generic holder as EN if en/fr missing.
    """
    out = {"en": [], "fr": []}
    if isinstance(rep_dict.get("en"), dict):
        t = rep_dict["en"].get("tasks")
        out["en"] = t if isinstance(t, list) else []
    if isinstance(rep_dict.get("fr"), dict):
        t = rep_dict["fr"].get("tasks")
        out["fr"] = t if isinstance(t, list) else []
    if not out["en"] and not out["fr"]:
        # fallback to generic
        for holder in ("report", "data"):
            v = lang_block(rep_dict, holder)
            t = v.get("tasks")
            if isinstance(t, list):
                out["en"] = t
                break
        if not out["en"] and isinstance(rep_dict.get("tasks"), list):
            out["en"] = rep_dict["tasks"]
    return out

def extract_lang_v01(rep_dict):
    """
    Return tables by language: {'en':[...], 'fr':[...]}
    Falls back to generic holder as EN if en/fr missing.
    """
    out = {"en": [], "fr": []}
    if isinstance(rep_dict.get("en"), dict):
        t = rep_dict["en"].get("tables")
        out["en"] = t if isinstance(t, list) else []
    if isinstance(rep_dict.get("fr"), dict):
        t = rep_dict["fr"].get("tables")
        out["fr"] = t if isinstance(t, list) else []
    if not out["en"] and not out["fr"]:
        for holder in ("report", "data"):
            v = lang_block(rep_dict, holder)
            t = v.get("tables")
            if isinstance(t, list):
                out["en"] = t
                break
        if not out["en"] and isinstance(rep_dict.get("tables"), list):
            out["en"] = rep_dict["tables"]
    return out

# --------------------- Aggregation ---------------------

def agg_v02(tasks):
    """
    Aggregate per-language metrics for v0.2 (tasks schema).
    Prefer task.stats.errors/rows; fall back to len(errors).
    valid_all = True if all task.valid True, False if any False, None if no boolean.
    """
    if not tasks:
        return {"error_count": 0, "row_count": 0, "valid_all": None, "warning_count": 0}
    err = 0
    rows = 0
    warns = 0
    valid_all = True
    saw_valid = False
    for t in tasks:
        stats = t.get("stats") or {}
        e_num = stats.get("errors")
        w_num = stats.get("warnings")
        r_num = stats.get("rows")
        # errors
        if isinstance(e_num, int):
            err += e_num
        else:
            err += len(t.get("errors") or [])
        # warnings
        if isinstance(w_num, int):
            warns += w_num
        else:
            warns += len(t.get("warnings") or [])
        # rows
        if isinstance(r_num, int):
            rows += r_num
        v = t.get("valid")
        if isinstance(v, bool):
            saw_valid = True
            if not v:
                valid_all = False
    if not saw_valid:
        valid_all = None
    return {"error_count": err, "row_count": rows, "valid_all": valid_all, "warning_count": warns}

def norm_get(table, *keys, default=None):
    for k in keys:
        if k in table:
            return table[k]
    for k in keys:
        alt = k.replace("-", "_") if "-" in k else k.replace("_", "-")
        if alt in table:
            return table[alt]
    return default

def agg_v01(tables):
    """
    Aggregate per-language metrics for v0.1 (tables schema).
    """
    if not tables:
        return {"error_count": 0, "row_count": 0, "valid_all": None, "warning_count": 0}
    err = 0
    rows = 0
    valid_all = True
    saw_valid = False
    for t in tables:
        err += int(norm_get(t, "error-count", "error_count", default=0) or 0)
        rows += int(norm_get(t, "row-count", "row_count", default=0) or 0)
        v = t.get("valid")
        if isinstance(v, bool):
            saw_valid = True
            if not v:
                valid_all = False
    if not saw_valid:
        valid_all = None
    return {"error_count": err, "row_count": rows, "valid_all": valid_all, "warning_count": 0}

# --------------------- Load & normalize ---------------------

def read_items(jsonl_path):
    items = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            o = ujson.loads(line)
            rep = parse_reports(o.get("reports"))
            version = detect_version(rep)
            created = (o.get("created") or "").strip()
            era = parse_created_era(created)

            # Language splits and aggregation per version
            if version == "v0.2":
                lang_data = extract_lang_v02(rep)
                en_aggr = agg_v02(lang_data["en"])
                fr_aggr = agg_v02(lang_data["fr"])
            elif version == "v0.1":
                lang_data = extract_lang_v01(rep)
                en_aggr = agg_v01(lang_data["en"])
                fr_aggr = agg_v01(lang_data["fr"])
            else:
                # unknown -> treat as empty
                lang_data = {"en": [], "fr": []}
                en_aggr = {"error_count": 0, "row_count": 0, "valid_all": None, "warning_count": 0}
                fr_aggr = {"error_count": 0, "row_count": 0, "valid_all": None, "warning_count": 0}

            def b(v): return None if v is None else bool(v)

            items.append({
                "id": (o.get("id") or o.get("resource_id") or ""),
                "resource_id": o.get("resource_id") or "",
                "created": created,
                "status": o.get("status") or "",
                "era": era,                    # new / old / unknown
                "version": version,            # v0.2 / v0.1 / unknown
                "en": {
                    "errors": en_aggr["error_count"], "rows": en_aggr["row_count"],
                    "valid": b(en_aggr["valid_all"]), "warnings": en_aggr["warning_count"]
                },
                "fr": {
                    "errors": fr_aggr["error_count"], "rows": fr_aggr["row_count"],
                    "valid": b(fr_aggr["valid_all"]), "warnings": fr_aggr["warning_count"]
                },
                "rep": rep,                    # for details
                "lang_data": lang_data,        # tasks or tables, version-specific
            })
    return items

# --------------------- UI assets ---------------------

CSS = """
:root{
  --bg:#0f1222; --panel:#161b33; --ink:#e8ecfa; --muted:#9aa4c2; --link:#96b5ff;
  --good:#26c281; --bad:#ff5f6d; --chip:#21284a; --line:rgba(255,255,255,.08)
}
*{box-sizing:border-box}
body{margin:0; background:var(--bg); color:var(--ink); font-family:Inter,system-ui,Segoe UI,Roboto,Arial}
a{color:var(--link); text-decoration:none} a:hover{text-decoration:underline}
.container{max-width:1280px; margin:0 auto; padding:28px}
.header{display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:16px}
.h1{font-size:20px; font-weight:700; letter-spacing:.2px}
.panel{background:var(--panel); border-radius:16px; box-shadow:0 12px 30px rgba(0,0,0,.25)}
.controls{display:flex; flex-wrap:wrap; gap:10px; padding:14px 16px; border-bottom:1px solid var(--line)}
.controls .grow{flex:1 1 260px}
.input, .select, .btn{
  height:36px; padding:0 10px; border:1px solid var(--line); background:rgba(255,255,255,.05);
  color:var(--ink); border-radius:10px; font-size:14px
}
.btn{cursor:pointer}
.table{width:100%; border-collapse:collapse; font-size:14px}
th, td{padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:top}
th{position:sticky; top:0; background:linear-gradient(0deg, rgba(22,27,51,.90), rgba(22,27,51,.95)); z-index:1}
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
"""

JS = r"""
let sortState = { key: null, dir: 1 }; // 1 asc, -1 desc
let pageSize = 25;
let currentPage = 1;

function getRows(){ return Array.from(document.querySelectorAll('tbody tr')); }
function visibleFilteredRows(){ return getRows().filter(r => r.dataset.filtered !== '0'); }

function getCellValue(row, key){
  if(key==='created')  return (row.dataset.created  || '').toLowerCase();
  if(key==='status')   return (row.dataset.status   || '').toLowerCase();
  if(key==='resource') return (row.dataset.resource || '').toLowerCase();
  if(key==='version')  return (row.dataset.version  || '').toLowerCase();
  if(key==='era')      return (row.dataset.era      || '').toLowerCase();
  return row.innerText.toLowerCase();
}

function sortBy(key){
  const tbody = document.querySelector('tbody');
  const rows = getRows();
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
  rows.forEach(r=>tbody.appendChild(r));
  updateSortIndicators();
  renderPage(1);
}

function updateSortIndicators(){
  document.querySelectorAll('th[data-sort]').forEach(th=>{
    const key=th.dataset.sort; th.querySelector('.sort-ind')?.remove();
    if(sortState.key===key){
      const s=document.createElement('span'); s.className='sort-ind'; s.textContent=sortState.dir===1?' ↑':' ↓'; th.appendChild(s);
    }
  });
}

function applyFilters(){
  const q   = (document.querySelector('#q')?.value || '').toLowerCase().trim();
  const rF  = (document.querySelector('#filter-resource')?.value || '').toLowerCase().trim();
  const sF  = (document.querySelector('#filter-status')?.value || '').toLowerCase().trim();
  const cF  = (document.querySelector('#filter-created')?.value || '').toLowerCase().trim();
  const vF  = (document.querySelector('#filter-version')?.value || '').toLowerCase().trim();
  const eF  = (document.querySelector('#filter-era')?.value || '').toLowerCase().trim();

  getRows().forEach(r=>{
    const text = r.innerText.toLowerCase();
    const dres = (r.dataset.resource || '').toLowerCase();
    const dsta = (r.dataset.status   || '').toLowerCase();
    const dcre = (r.dataset.created  || '').toLowerCase();
    const dver = (r.dataset.version  || '').toLowerCase();
    const dera = (r.dataset.era      || '').toLowerCase();

    let show = true;
    if(q && !text.includes(q)) show = false;
    if(rF && !dres.includes(rF)) show = false;
    if(sF && dsta !== sF) show = false;
    if(cF && !dcre.includes(cF)) show = false;
    if(vF && dver !== vF) show = false;
    if(eF && dera !== eF) show = false;

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

function setPageSize(v){ pageSize = parseInt(v,10)||25; renderPage(1); }
function gotoPrev(){ renderPage(currentPage-1); }
function gotoNext(){ renderPage(currentPage+1); }

function renderPage(page){
  const rows = visibleFilteredRows();
  const total = rows.length;
  const totalPages = Math.max(1, Math.ceil(total/pageSize));
  currentPage = Math.max(1, Math.min(page, totalPages));
  getRows().forEach(r=> r.style.display='none');
  const start=(currentPage-1)*pageSize, end=start+pageSize;
  rows.slice(start,end).forEach(r=> r.style.display='');
  const info = document.querySelector('#pager-info');
  if(info){
    const shownStart = total ? (start+1) : 0;
    const shownEnd = Math.min(end, total);
    info.textContent = `${shownStart}-${shownEnd} of ${total}`;
  }
}

window.addEventListener('DOMContentLoaded',()=>{
  setLang(localStorage.getItem('vr_lang')||'en');
  ['#q','#filter-resource','#filter-status','#filter-created','#filter-version','#filter-era'].forEach(sel=>{
    const el=document.querySelector(sel); if(!el) return;
    el.addEventListener('input', applyFilters);
    el.addEventListener('change', applyFilters);
  });
  const ps=document.querySelector('#page-size');
  if(ps) ps.addEventListener('change', e=> setPageSize(e.target.value));

  // initialize
  getRows().forEach(r=> r.dataset.filtered='1');
  updateSortIndicators();
  setPageSize(document.querySelector('#page-size')?.value || 25);
  renderPage(1);
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

# --------------------- Index page ---------------------

def write_index(items, out_dir):
    def row_html(it):
        link=f"reports/{slugify(it['id'])}.html"
        created = it['created'] or ''
        status  = it['status'] or ''
        resource= it['resource_id'] or '-'
        version = it['version'] or 'unknown'
        era     = it['era'] or 'unknown'

        en_badge = badge_state(it['en']['valid']) + f' {chip(f"EN errors: {it["en"]["errors"]}", "na")}'
        fr_badge = badge_state(it['fr']['valid']) + f' {chip(f"FR erreurs: {it["fr"]["errors"]}", "na")}'

        ver_chip= chip(version, "na")
        era_chip= chip(era, "na")
        st_chip = chip(status, 'na' if status not in ('success','failure') else ('ok' if status=='success' else 'bad'))

        return f"""
          <tr data-created="{html.escape(created)}"
              data-status="{html.escape(status.lower())}"
              data-resource="{html.escape(resource)}"
              data-version="{html.escape(version.lower())}"
              data-era="{html.escape(era.lower())}"
              data-filtered="1">
            <td><a href="{link}"><code>{html.escape(it['id'])}</code></a><div class="subtle">{ver_chip} {era_chip}</div></td>
            <td><code>{html.escape(resource)}</code></td>
            <td>{en_badge}</td>
            <td>{fr_badge}</td>
            <td>{st_chip}</td>
            <td><time>{html.escape(created)}</time></td>
          </tr>
        """

    rows = [row_html(it) for it in items]

    html_index = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Validation Reports</title>
<link rel="stylesheet" href="style.css"><script defer src="app.js"></script>
</head><body>
  <div class="container">
    <div class="header">
      <div class="h1">Validation Reports</div>
      <div class="lang-toggle">
        <button type="button" class="btn" data-set="en" onclick="setLang('en')">EN</button>
        <button type="button" class="btn" data-set="fr" onclick="setLang('fr')">FR</button>
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
        <div><label class="subtle">Era<br/><select class="select" id="filter-era">
          <option value="">All</option>
          <option value="new">new (≥ 2024-12-10)</option>
          <option value="old">old (&lt; 2024-12-10)</option>
          <option value="unknown">unknown</option>
        </select></label></div>
      </div>

      <div class="table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>ID</th>

              <th data-sort="resource" onclick="sortBy('resource')">
                Resource
                <div class="hdr-ctrl"><input class="input" id="filter-resource" placeholder="Filter resource…"/></div>
              </th>

              <th>EN</th>
              <th>FR</th>

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
      <strong>v0.2</strong> = tasks schema (priority). <strong>v0.1</strong> = tables schema (legacy).
      Era is inferred from the created timestamp; rendering depends on the detected version.
    </p>
  </div>
</body></html>"""

    with open(os.path.join(out_dir,"index.html"), "w", encoding="utf-8") as f:
        f.write(html_index)

# --------------------- Detail: version-specific templates ---------------------

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

# --- v0.2 (tasks) language panel
def render_task_block(task, lang='en', idx=0):
    stats = task.get("stats") or {}
    labels = task.get("labels") or []
    warns  = task.get("warnings") or []
    errs   = task.get("errors") or []
    name   = task.get("name") or f"Task {idx+1}"
    ttype  = task.get("type") or ""
    place  = task.get("place") or ""

    kv_parts = []
    kv_parts.append(f"<div>Valid</div><div>{badge_state(task.get('valid'))}</div>")
    for label, key in [
        ("Type", "type"), ("Source", "place"),
        ("Rows", "rows"), ("Fields", "fields"),
        ("Errors", "errors"), ("Warnings", "warnings"),
        ("Bytes", "bytes"), ("MD5", "md5"), ("SHA256", "sha256"),
        ("Seconds", "seconds"),
    ]:
        val = stats.get(key) if key in ("rows","fields","errors","warnings","bytes","md5","sha256","seconds") else task.get(key)
        if val not in (None, "", []):
            if key == "place":
                val = f'<a href="{html.escape(str(val))}" target="_blank" rel="noopener">{html.escape(str(val))}</a>'
            kv_parts.append(f"<div>{label}</div><div>{val}</div>")

    labels_html = "<br/>".join(html.escape(str(x)) for x in labels) if labels else '<span class="subtle">(none)</span>'
    warns_html  = "<br/>".join(html.escape(str(x)) for x in warns)  if warns  else '<span class="subtle">(none)</span>'
    errs_html   = "<br/>".join(html.escape(str(x)) for x in errs)   if errs   else '<span class="subtle">(none)</span>'

    raw_json = html.escape(json.dumps(task, ensure_ascii=False, indent=2))

    return f"""
    <div class="section">
      <div class="h1" style="font-size:16px">{html.escape(name)} {chip(ttype, 'na')}</div>
      <div class="kv" style="margin-top:8px">{''.join(kv_parts)}</div>

      <h4 style="margin:12px 0 6px">{'Labels' if lang=='en' else 'Étiquettes'}</h4>
      <div class="code">{labels_html}</div>

      <h4 style="margin:12px 0 6px">{'Warnings' if lang=='en' else 'Avertissements'}</h4>
      <div class="code">{warns_html}</div>

      <h4 style="margin:12px 0 6px">{'Errors' if lang=='en' else 'Erreurs'}</h4>
      <div class="code">{errs_html}</div>

      <h4 style="margin:12px 0 6px">{'Raw task JSON' if lang=='en' else 'JSON brut de la tâche'}</h4>
      <div class="code">{raw_json}</div>
    </div>
    """

def render_lang_panel_v02(lang, tasks):
    if tasks is None:
        return f'<div class="section"><span class="badge na">Not available</span></div>'
    aggr = agg_v02(tasks)
    head = f"""
      <div class="section">
        <div class="kv">
          <div>{"Valid" if lang=='en' else "Valide"}</div><div>{badge_state(aggr["valid_all"])}</div>
          <div>{"Errors" if lang=='en' else "Erreurs"}</div><div>{aggr["error_count"]}</div>
          <div>{"Warnings" if lang=='en' else "Avertissements"}</div><div>{aggr["warning_count"]}</div>
          <div>{"Rows" if lang=='en' else "Lignes"}</div><div>{aggr["row_count"]}</div>
        </div>
      </div>
    """
    blocks = "".join(render_task_block(t, lang, i) for i, t in enumerate(tasks))
    if not blocks:
        blocks = '<div class="section"><span class="badge na">No tasks</span></div>' if lang=='en' \
               else '<div class="section"><span class="badge na">Aucune tâche</span></div>'
    style = '' if lang=='en' else 'style="display:none"'
    return f'<section data-lang="{lang}" {style} class="panel">{head}{blocks}</section>'

# --- v0.1 (tables) language panel
def render_table_block_v01(t, lang='en', idx=0):
    headers = t.get("headers", [])
    header_text = html.escape("\n".join(map(str, headers))) if headers else '<span class="subtle">(none)</span>'
    kv_parts = []
    # Show common v0.1 descriptors
    for label, key in [
        ("Valid","valid"), ("Format","format"), ("Encoding","encoding"), ("Scheme","scheme"),
        ("Source","source"), ("Time","time"),
        ("Row count","row-count"), ("Row count","row_count"),
        ("Error count","error-count"), ("Error count","error_count"),
    ]:
        val = t.get(key)
        if label in ("Row count","Error count") and val is None:
            # try normalized
            alt = key.replace("-", "_") if "-" in key else key.replace("_", "-")
            val = t.get(alt)
        if val not in (None, "", []):
            if key == "valid":
                val = badge_state(bool(val))
            kv_parts.append(f"<div>{label}</div><div>{html.escape(str(val))}</div>")

    errs = t.get("errors", [])
    errors_table = render_errors_table(errs, lang)

    raw_json = html.escape(json.dumps(t, ensure_ascii=False, indent=2))
    return f"""
      <div class="section">
        <div class="h1" style="font-size:16px">Table {idx+1}</div>
        <div class="kv" style="margin-top:8px">{''.join(kv_parts) or '<div>Table</div><div>-</div>'}</div>
        <h4 style="margin:12px 0 6px">{'Errors' if lang=='en' else 'Erreurs'}</h4>
        {errors_table}
        <h4 style="margin:12px 0 6px">{'Headers' if lang=='en' else 'En-têtes'}</h4>
        <div class="code">{header_text}</div>
        <h4 style="margin:12px 0 6px">{'Raw table JSON' if lang=='en' else 'JSON brut de la table'}</h4>
        <div class="code">{raw_json}</div>
      </div>
    """

def render_lang_panel_v01(lang, tables):
    if tables is None:
        return f'<div class="section"><span class="badge na">Not available</span></div>'
    aggr = agg_v01(tables)
    head = f"""
      <div class="section">
        <div class="kv">
          <div>{"Valid" if lang=='en' else "Valide"}</div><div>{badge_state(aggr["valid_all"])}</div>
          <div>{"Errors" if lang=='en' else "Erreurs"}</div><div>{aggr["error_count"]}</div>
          <div>{"Rows" if lang=='en' else "Lignes"}</div><div>{aggr["row_count"]}</div>
        </div>
      </div>
    """
    blocks = "".join(render_table_block_v01(t, lang, i) for i, t in enumerate(tables))
    if not blocks:
        blocks = '<div class="section"><span class="badge na">No tables</span></div>' if lang=='en' \
               else '<div class="section"><span class="badge na">Aucune table</span></div>'
    style = '' if lang=='en' else 'style="display:none"'
    return f'<section data-lang="{lang}" {style} class="panel">{head}{blocks}</section>'

# --- detail page wrapper

def write_report_pages(items, out_dir):
    rdir = os.path.join(out_dir, "reports")
    os.makedirs(rdir, exist_ok=True)
    for it in items:
        pid = slugify(it['id'])
        ver = it.get("version") or "unknown"

        if ver == "v0.2":
            body = f"""
            {render_lang_panel_v02('en', it['lang_data'].get('en'))}
            {render_lang_panel_v02('fr', it['lang_data'].get('fr'))}
            """
        elif ver == "v0.1":
            body = f"""
            {render_lang_panel_v01('en', it['lang_data'].get('en'))}
            {render_lang_panel_v01('fr', it['lang_data'].get('fr'))}
            """
        else:
            body = '<div class="panel section"><span class="badge na">Unknown report format</span></div>'

        page = f"""<!doctype html><html lang="en"><head>
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
      <div class="kv" style="margin-top:8px">
        <div>Version</div><div>{chip(ver, 'na')}</div>
        <div>Era</div><div>{chip(it['era'], 'na')}</div>
        <div>Resource</div><div><code>{html.escape(it['resource_id'] or "-")}</code></div>
        <div>Status</div><div>{chip(it['status'] or 'unknown', 'na' if it['status'] not in ('success','failure') else ('ok' if it['status']=='success' else 'bad'))}</div>
        <div>Created</div><div><time>{html.escape(it['created'] or '')}</time></div>
      </div>
    </div>

    {body}
  </div>
</body></html>"""
        with open(os.path.join(rdir, f"{pid}.html"), "w", encoding="utf-8") as f:
            f.write(page)

# --------------------- Main ---------------------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "style.css"), "w", encoding="utf-8") as f: f.write(CSS)
    with open(os.path.join(OUT_DIR, "app.js"),   "w", encoding="utf-8") as f: f.write(JS)
    items = read_items(IN_PATH)
    # Index last (needs items)
    write_index(items, OUT_DIR)
    write_report_pages(items, OUT_DIR)
    print(f"✓ Site built: {OUT_DIR}/  reports: {len(items)}")

if __name__ == "__main__":
    main()
