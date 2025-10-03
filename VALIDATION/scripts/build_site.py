#!/usr/bin/env python3
"""
Build a static site from validation.jsonl (ckanext-validation Frictionless reports),
robust across historical format variants.

INPUT:
  VALIDATION_JSONL   default: validation.jsonl
OUTPUT:
  SITE_DIR           default: VALIDATION
"""

import os, re, json, html, ujson

IN_PATH  = os.getenv("VALIDATION_JSONL", "validation.jsonl")
OUT_DIR  = os.getenv("SITE_DIR", "VALIDATION")

# ---------- parsing helpers ----------

def slugify(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+","-", s or "").strip("-") or "report"

def parse_reports(val):
    """Unwrap JSON-encoded strings (up to 3 layers) and return a dict or {}."""
    seen = 0
    while isinstance(val, str) and seen < 3:
        try:
            val = ujson.loads(val)
        except ValueError:
            break
        seen += 1
    return val if isinstance(val, dict) else {}

def first_existing(d, keys, default=None):
    """Return d[k] for first k in keys that exists and is a dict; else default."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, dict):
            return k, v
    return None, default

def get_tables(rep_dict, lang_key=None):
    """Return a list of tables for the given language (or top-level), robustly."""
    if lang_key and isinstance(rep_dict.get(lang_key), dict):
        t = rep_dict[lang_key].get("tables")
        return t if isinstance(t, list) else []
    # fallback: try top-level tables if language-specific is absent
    t = rep_dict.get("tables")
    return t if isinstance(t, list) else []

def norm_get(table, *keys, default=None):
    """Fetch a value from a table handling hyphen/underscore variants."""
    for k in keys:
        if k in table:
            return table[k]
    # try swapping hyphen <-> underscore
    for k in keys:
        alt = k.replace("-", "_") if "-" in k else k.replace("_", "-")
        if alt in table:
            return table[alt]
    return default

def aggregate_table_metrics(tables):
    """Aggregate counts/valid across multiple tables."""
    if not tables:
        return {
            "error_count": 0,
            "row_count": 0,
            "valid_all": None,  # None means unknown
        }
    error_total = 0
    row_total = 0
    valid_all = True
    saw_valid = False
    for t in tables:
        error_total += int(norm_get(t, "error-count", "error_count", default=0) or 0)
        row_total   += int(norm_get(t, "row-count", "row_count", default=0) or 0)
        v = t.get("valid")
        if isinstance(v, bool):
            saw_valid = True
            valid_all = valid_all and v
        else:
            # if any table lacks valid, treat as unknown unless all others were True
            pass
    if not saw_valid:
        valid_all = None
    return {"error_count": error_total, "row_count": row_total, "valid_all": valid_all}

def language_blocks(rep):
    """
    Return a dict of language -> tables list, supporting:
      - 'en', 'fr' languages
      - legacy holders like 'report' or 'data'
      - absence of one or both languages
    """
    langs = {}
    # Prefer explicit en/fr if present
    for lang in ("en", "fr"):
        if isinstance(rep.get(lang), dict):
            langs[lang] = get_tables(rep, lang)

    # If no explicit languages found, try a generic block once
    if not langs:
        generic_key, generic = first_existing(rep, ("report", "data"), default=None)
        if generic:
            # Treat it as EN for display; FR will be "not available"
            tables = generic.get("tables") if isinstance(generic.get("tables"), list) else []
            langs["en"] = tables

    # Guarantee keys so UI logic is simple (may be empty to signal "N/A")
    langs.setdefault("en", langs.get("en", []))
    langs.setdefault("fr", langs.get("fr", []))
    return langs

# ---------- read & normalize all records ----------

def read_items(jsonl_path):
    items = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = ujson.loads(line)
            rep = parse_reports(obj.get("reports"))

            # map languages -> table lists
            lang_tables = language_blocks(rep)
            en_tables = lang_tables.get("en", [])
            fr_tables = lang_tables.get("fr", [])

            en_aggr = aggregate_table_metrics(en_tables)
            fr_aggr = aggregate_table_metrics(fr_tables)

            def v_to_bool(v):
                return None if v is None else bool(v)

            items.append({
                "id": (obj.get("id") or obj.get("resource_id") or ""),
                "resource_id": obj.get("resource_id") or "",
                "created": obj.get("created") or "",
                "status": obj.get("status") or "",
                "errors_en": en_aggr["error_count"],
                "errors_fr": fr_aggr["error_count"],
                "valid_en": v_to_bool(en_aggr["valid_all"]),
                "valid_fr": v_to_bool(fr_aggr["valid_all"]),
                "rep": rep,
                "lang_tables": lang_tables,  # keep full tables for detail page
            })
    return items

# ---------- UI assets ----------

CSS = """
:root{--bg:#0b1020;--card:#121a35;--text:#e8eefc;--sub:#a9b4d0;--ok:#1fa971;--bad:#ff5555;--muted:#6b7280;--link:#8ab4ff}
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Arial;background:var(--bg);color:var(--text);margin:0}
a{color:var(--link);text-decoration:none} a:hover{text-decoration:underline}
.container{max-width:1200px;margin:0 auto;padding:24px}
.card{background:var(--card);border-radius:16px;padding:20px;box-shadow:0 10px 30px rgba(0,0,0,.2)}
.table{width:100%;border-collapse:collapse;font-size:14px}
.table th,.table td{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06);vertical-align:top}
.table th{position:sticky;top:0;background:linear-gradient(0deg,rgba(18,26,53,.9),rgba(18,26,53,.95));backdrop-filter:saturate(180%) blur(5px);z-index:1}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-weight:600;font-size:12px}
.badge.ok{background:rgba(31,169,113,.2);color:var(--ok)} .badge.bad{background:rgba(255,85,85,.18);color:var(--bad)} .badge.muted{background:rgba(255,255,255,.08);color:var(--sub)}
.badge.na{background:rgba(255,255,255,.08);color:var(--muted)}
.toolbar{display:flex;gap:12px;align-items:center;margin:8px 0 16px}
input[type="search"]{width:320px;max-width:60vw;padding:10px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);color:var(--text)}
.lang-toggle button{padding:8px 12px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);color:var(--text);border-radius:10px}
.lang-toggle button.active{outline:2px solid var(--link)}
.kv{display:grid;grid-template-columns:180px 1fr;gap:8px;font-size:14px}
.code{font-family:ui-monospace,Menlo,Consolas,monospace;background:rgba(255,255,255,.06);padding:10px;border-radius:10px;overflow:auto;white-space:pre-wrap}
.footer{margin-top:20px;font-size:12px;color:var(--muted)}
.details{margin:10px 0;padding:8px 10px;border-radius:10px;background:rgba(255,255,255,.04)}
th .hdr-ctrl{display:block; margin-top:6px; font-weight:400}
th .hdr-ctrl input, th .hdr-ctrl select{
  width:100%; padding:6px 8px; border-radius:8px;
  border:1px solid rgba(255,255,255,.12); background:rgba(255,255,255,.06); color:var(--text);
  font-size:12px;
}
.sort-ind{opacity:.8}
th[data-sort]{cursor:pointer; user-select:none}
"""

JS = r"""
let sortState = { key: null, dir: 1 }; // 1 asc, -1 desc

function getCellValue(row, key){
  if(key==='created'){
    return (row.dataset.created || '').toLowerCase();
  } else if(key==='status'){
    return (row.dataset.status || '').toLowerCase();
  } else if(key==='resource'){
    return (row.dataset.resource || '').toLowerCase();
  } else {
    return row.innerText.toLowerCase();
  }
}

function sortBy(key){
  const tbody = document.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  sortState.dir = (sortState.key === key) ? -sortState.dir : 1;
  sortState.key = key;

  rows.sort((a,b)=>{
    const va = getCellValue(a, key);
    const vb = getCellValue(b, key);
    if(key==='created'){
      const da = Date.parse(va) || 0, db = Date.parse(vb) || 0;
      if(da !== db) return (da - db) * sortState.dir;
    }
    return va.localeCompare(vb) * sortState.dir;
  });

  rows.forEach(r=>tbody.appendChild(r));
  updateSortIndicators();
}

function updateSortIndicators(){
  document.querySelectorAll('th[data-sort]').forEach(th=>{
    const key = th.dataset.sort;
    th.querySelector('.sort-ind')?.remove();
    if(sortState.key===key){
      const s = document.createElement('span');
      s.className='sort-ind';
      s.textContent = sortState.dir===1 ? ' ↑' : ' ↓';
      th.appendChild(s);
    }
  });
}

function applyFilters(){
  const q = (document.querySelector('#q')?.value || '').toLowerCase().trim();

  const resFilter = (document.querySelector('#filter-resource')?.value || '').toLowerCase().trim();
  const statusFilter = (document.querySelector('#filter-status')?.value || '').toLowerCase().trim(); // '', 'success', 'failure'
  const createdText = (document.querySelector('#filter-created')?.value || '').toLowerCase().trim();

  document.querySelectorAll('tbody tr').forEach(r=>{
    const text = r.innerText.toLowerCase();
    const dres = (r.dataset.resource || '').toLowerCase();
    const dstat= (r.dataset.status || '').toLowerCase();
    const dcre = (r.dataset.created || '').toLowerCase();

    let show = true;
    if(q && !text.includes(q)) show = false;
    if(resFilter && !dres.includes(resFilter)) show = false;
    if(statusFilter && dstat !== statusFilter) show = false;
    if(createdText && !dcre.includes(createdText)) show = false;

    r.style.display = show ? '' : 'none';
  });
}

function filterTable(){ applyFilters(); } // keep compatibility with the existing search box

function setLang(lang){
  localStorage.setItem('vr_lang',lang);
  document.querySelectorAll('[data-lang]').forEach(el=>{
    el.style.display = (el.dataset.lang===lang)?'':'none';
  });
  document.querySelectorAll('.lang-toggle button').forEach(b=>{
    b.classList.toggle('active', b.dataset.set===lang);
  });
}

window.addEventListener('DOMContentLoaded',()=>{
  setLang(localStorage.getItem('vr_lang')||'en');
  ['#filter-resource','#filter-status','#filter-created','#q'].forEach(sel=>{
    const el = document.querySelector(sel);
    if(!el) return;
    el.addEventListener('input', applyFilters);
    el.addEventListener('change', applyFilters);
  });
  updateSortIndicators();
});
"""

def badge_state(ok):
    if ok is True:
        return '<span class="badge ok">OK</span>'
    if ok is False:
        return '<span class="badge bad">FAIL</span>'
    return '<span class="badge na">N/A</span>'

# ---------- write index ----------

def write_index(items, out_dir):
    def row_html(it):
        link=f"reports/{slugify(it['id'])}.html"
        en_badge = badge_state(it['valid_en']) + f' <span class="badge muted">EN errors: {it["errors_en"]}</span>'
        fr_badge = badge_state(it['valid_fr']) + f' <span class="badge muted">FR erreurs: {it["errors_fr"]}</span>'
        created = (it['created'] or '')
        status  = (it['status'] or '')
        resource= (it['resource_id'] or '-')
        return f"""
          <tr data-created="{html.escape(created)}"
              data-status="{html.escape(status.lower())}"
              data-resource="{html.escape(resource)}">
            <td><a href="{link}"><code>{html.escape(it['id'])}</code></a></td>
            <td><code>{html.escape(resource)}</code></td>
            <td>{en_badge}</td>
            <td>{fr_badge}</td>
            <td><span class="badge {'ok' if status=='success' else 'bad'}">{html.escape(status)}</span></td>
            <td><time>{html.escape(created)}</time></td>
          </tr>
        """

    rows = [row_html(it) for it in items]

    html_index = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Validation Reports (Static)</title>
<link rel="stylesheet" href="style.css"><script defer src="app.js"></script>
</head><body><div class="container"><div class="card">
  <h1 data-lang="en">Validation Reports</h1>
  <h1 data-lang="fr" style="display:none">Rapports de validation</h1>
  <div class="toolbar">
    <input type="search" id="q" placeholder="Filter…" oninput="filterTable()"/>
    <div class="lang-toggle">
      <button type="button" data-set="en" onclick="setLang('en')" class="active">EN</button>
      <button type="button" data-set="fr" onclick="setLang('fr')">FR</button>
    </div>
  </div>

  <table class="table">
    <thead>
      <tr>
        <th>ID</th>

        <th data-sort="resource" onclick="sortBy('resource')">
          Resource
          <div class="hdr-ctrl">
            <input id="filter-resource" placeholder="Filter resource…"/>
          </div>
        </th>

        <th>EN</th>
        <th>FR</th>

        <th data-sort="status" onclick="sortBy('status')">
          Status
          <div class="hdr-ctrl">
            <select id="filter-status">
              <option value="">All</option>
              <option value="success">success</option>
              <option value="failure">failure</option>
            </select>
          </div>
        </th>

        <th data-sort="created" onclick="sortBy('created')">
          Created
          <div class="hdr-ctrl">
            <input id="filter-created" placeholder="yyyy-mm… or text"/>
          </div>
        </th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>

  <div class="footer" data-lang="en">Pre-rendered static site generated from CKAN validation JSONL.</div>
  <div class="footer" data-lang="fr" style="display:none">Site statique pré-rendu à partir des validations CKAN (JSONL).</div>
</div></div></body></html>"""

    with open(os.path.join(out_dir,"index.html"), "w", encoding="utf-8") as f:
        f.write(html_index)

# ---------- write detail pages ----------

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

def render_lang_section(lang, tables):
    if tables is None or not isinstance(tables, list):
        return f'<section class="details"><span class="badge na">Not available</span></section>'
    # aggregate for header
    aggr = aggregate_table_metrics(tables)
    ok_html = badge_state(aggr["valid_all"])
    meta = f"""
    <div class="kv">
      <div>{"Valid" if lang=="en" else "Valide"}</div><div>{ok_html}</div>
      <div>{"Errors" if lang=="en" else "Erreurs"}</div><div>{aggr["error_count"]}</div>
      <div>{"Rows" if lang=="en" else "Lignes"}</div><div>{aggr["row_count"]}</div>
    </div>
    """
    # per-table details
    parts=[]
    for i, t in enumerate(tables):
        headers = t.get("headers", [])
        header_text = html.escape("\n".join(map(str, headers)))
        raw_json = html.escape(json.dumps(t, ensure_ascii=False, indent=2))
        fmt = t.get("format") or ""
        src = t.get("source") or ""
        errs = t.get("errors", [])
        title = f"Table {i+1}"
        parts.append(f"""
        <details class="details">
          <summary>{title} — format: {html.escape(str(fmt))} — source: {html.escape(str(src))}</summary>
          <h3 style="margin-top:8px">{'Errors' if lang=='en' else 'Erreurs'}</h3>
          {render_errors_table(errs, lang)}
          <details style="margin-top:8px"><summary>{'Headers' if lang=='en' else 'En-têtes'}</summary><div class="code">{header_text}</div></details>
          <details style="margin-top:8px"><summary>{'Raw table JSON' if lang=='en' else 'JSON brut de la table'}</summary><div class="code">{raw_json}</div></details>
        </details>
        """)
    return f"""
    <section data-lang="{lang}" style="display:{'' if lang=='en' else 'none'}">
      <h2>{"Validation Result" if lang=='en' else "Résultat de la validation"}</h2>
      {meta}
      {''.join(parts) if parts else ('<p class="badge na">No tables</p>' if lang=='en' else '<p class="badge na">Aucune table</p>')}
    </section>
    """

def write_report_pages(items, out_dir):
    rdir = os.path.join(out_dir, "reports")
    os.makedirs(rdir, exist_ok=True)
    for it in items:
        pid = slugify(it['id'])
        lang_tables = it["lang_tables"]
        en_tables = lang_tables.get("en", [])
        fr_tables = lang_tables.get("fr", [])
        page = f"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Report {html.escape(it['id'])}</title>
<link rel="stylesheet" href="../style.css"><script defer src="../app.js"></script>
</head><body><div class="container"><div class="card">
  <div class="toolbar" style="justify-content:space-between">
    <div>
      <a href="../index.html">← Back</a>
      <h1 style="margin-top:6px"><code>{html.escape(it['id'])}</code></h1>
      <div class="kv" style="margin-top:8px">
        <div>Resource</div><div><code>{html.escape(it['resource_id'] or "-")}</code></div>
        <div>Status</div><div><span class="badge {'ok' if it['status']=='success' else 'bad'}">{html.escape(it['status'])}</span></div>
        <div>Created</div><div><time>{html.escape(it['created'] or '')}</time></div>
      </div>
    </div>
    <div class="lang-toggle">
      <button type="button" data-set="en" onclick="setLang('en')" class="active">EN</button>
      <button type="button" data-set="fr" onclick="setLang('fr')">FR</button>
    </div>
  </div>
  {render_lang_section('en', en_tables)}
  {render_lang_section('fr', fr_tables)}
</div></div></body></html>"""
        with open(os.path.join(rdir, f"{pid}.html"), "w", encoding="utf-8") as f:
            f.write(page)

# ---------- main ----------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "style.css"), "w", encoding="utf-8") as f: f.write(CSS)
    with open(os.path.join(OUT_DIR, "app.js"),   "w", encoding="utf-8") as f: f.write(JS)
    items = read_items(IN_PATH)
    write_index(items, OUT_DIR)
    write_report_pages(items, OUT_DIR)
    print(f"✓ Site built: {OUT_DIR}/ (reports: {len(items)})")

if __name__ == "__main__":
    main()
