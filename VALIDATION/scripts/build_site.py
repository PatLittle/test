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

IN_PATH  = os.getenv("VALIDATION_JSONL", "validation_enriched.jsonl")
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
.h1{font-size:20px; font-weight:700; letter-spacing:.2px}
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
"""

JS = r"""
let sortState = { key: null, dir: 1 }; // 1 asc, -1 desc
let pageSize = 25;
let currentPage = 1;

function getRows(){ return Array.from(document.querySelectorAll('tbody tr')); }
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

function setPageSize(v){ pageSize = parseInt(v,10)||25; renderPage(1); }
function gotoPrev(){ renderPage(currentPage-1); }
function gotoNext(){ renderPage(currentPage+1); }

function renderPage(page){
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

def lang_html(en, fr):
    return (f'<span data-lang="en">{html.escape(en or "")}</span>'
            f'<span data-lang="fr" style="display:none">{html.escape(fr or "")}</span>')

# --------------------- Index page ---------------------

def write_index(items, out_dir):
    def row_html(it):
        link=f"reports/{slugify(it['id'])}.html"
        created = it['created'] or ''
        status  = it['status'] or ''
        resource_code = it['resource_id'] or '-'
        version = it['version'] or 'unknown'
        org     = it['organization_name'] or ''
        d_en, d_fr = it.get("dataset_title_en",""), it.get("dataset_title_fr","")
        r_en, r_fr = it.get("resource_name_en",""), it.get("resource_name_fr","")

        # one Errors column driven by language
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
      </div>

      <div class="table-wrap">
        <table class="table">
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

def write_report_pages(items, out_dir):
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

        header_meta=f"""
        <div class="kv" style="margin-top:8px">
          <div>Version</div><div>{chip(ver,'na')}</div>
          <div>Organization</div><div>{html.escape(it.get('organization_name',''))}</div>
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

# --------------------- Main ---------------------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "style.css"), "w", encoding="utf-8") as f: f.write(CSS)
    with open(os.path.join(OUT_DIR, "app.js"),   "w", encoding="utf-8") as f: f.write(JS)
    items=read_items(IN_PATH)
    write_index(items, OUT_DIR)
    write_report_pages(items, OUT_DIR)
    print(f"✓ Site built: {OUT_DIR}/  reports: {len(items)}")

if __name__ == "__main__":
    main()
