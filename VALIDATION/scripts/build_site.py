#!/usr/bin/env python3
"""
Build a static site from validation.jsonl (ckanext-validation Frictionless reports).
INPUT:
  VALIDATION_JSONL   default: validation.jsonl
OUTPUT:
  SITE_DIR           default: site/
"""

import os, re, json, html, ujson

IN_PATH  = os.getenv("VALIDATION_JSONL", "validation.jsonl")
OUT_DIR  = os.getenv("SITE_DIR", "VALIDATION")  # <— was "site"

def slugify(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+","-", s or "").strip("-") or "report"

def parse_reports(val):
    # Some sources store JSON as double-encoded string
    seen = 0
    while isinstance(val, str) and seen < 3:
        try:
            val = ujson.loads(val)
        except ValueError:
            break
        seen += 1
    return val if isinstance(val, dict) else {}

def read_items(jsonl_path):
    items = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = ujson.loads(line)
            rep = parse_reports(obj.get("reports"))
            en  = rep.get("en", {})
            fr  = rep.get("fr", {})
            t_en = (en.get("tables") or [{}])
            t_fr = (fr.get("tables") or [{}])
            t0_en= t_en[0] if t_en else {}
            t0_fr= t_fr[0] if t_fr else {}

            items.append({
                "id": obj.get("id") or obj.get("resource_id") or "",
                "resource_id": obj.get("resource_id") or "",
                "created": obj.get("created") or "",
                "status": obj.get("status") or "",
                "errors_en": int(t0_en.get("error-count") or 0),
                "errors_fr": int(t0_fr.get("error-count") or 0),
                "valid_en": bool(t0_en.get("valid")),
                "valid_fr": bool(t0_fr.get("valid")),
                "rep": rep,
            })
    return items

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
.toolbar{display:flex;gap:12px;align-items:center;margin:8px 0 16px}
input[type="search"]{width:320px;max-width:60vw;padding:10px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);color:var(--text)}
.lang-toggle button{padding:8px 12px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.06);color:var(--text);border-radius:10px}
.lang-toggle button.active{outline:2px solid var(--link)}
.kv{display:grid;grid-template-columns:180px 1fr;gap:8px;font-size:14px}
.code{font-family:ui-monospace,Menlo,Consolas,monospace;background:rgba(255,255,255,.06);padding:10px;border-radius:10px;overflow:auto;white-space:pre-wrap}
.footer{margin-top:20px;font-size:12px;color:var(--muted)}
"""

JS = """
function filterTable(){
  const q=document.querySelector('#q').value.toLowerCase().trim();
  document.querySelectorAll('tbody tr').forEach(r=>{
    r.style.display = r.innerText.toLowerCase().includes(q)?'':'none';
  });
}
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
});
"""

def badge(ok): return f'<span class="badge {"ok" if ok else "bad"}">{"OK" if ok else "FAIL"}</span>'

def write_index(items, out_dir):
    rows=[]
    for it in items:
        link=f"reports/{slugify(it['id'])}.html"
        rows.append(f"""
          <tr>
            <td><a href="{link}"><code>{html.escape(it['id'])}</code></a></td>
            <td><code>{html.escape(it['resource_id'] or "-")}</code></td>
            <td>{badge(it['valid_en'])} <span class="badge muted">EN errors: {it['errors_en']}</span></td>
            <td>{badge(it['valid_fr'])} <span class="badge muted">FR erreurs: {it['errors_fr']}</span></td>
            <td><span class="badge {'ok' if it['status']=='success' else 'bad'}">{html.escape(it['status'])}</span></td>
            <td><time>{html.escape(it['created'] or '')}</time></td>
          </tr>
        """)
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
    <thead><tr><th>ID</th><th>Resource</th><th>EN</th><th>FR</th><th>Status</th><th>Created</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <div class="footer" data-lang="en">Pre-rendered static site generated from CKAN validation JSONL.</div>
  <div class="footer" data-lang="fr" style="display:none">Site statique pré-rendu à partir des validations CKAN (JSONL).</div>
</div></div></body></html>"""
    with open(os.path.join(out_dir,"index.html"), "w", encoding="utf-8") as f:
        f.write(html_index)

def render_errors(errs, lang='en'):
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

def write_report_pages(items, out_dir):
    rdir = os.path.join(out_dir, "reports")
    os.makedirs(rdir, exist_ok=True)
    for it in items:
        pid = slugify(it['id'])
        rep = it['rep']
        sections = []
        for lang in ('en','fr'):
            lang_obj = rep.get(lang, {})
            tables = lang_obj.get('tables', [])
            t0 = tables[0] if tables else {}
            errors = t0.get('errors', [])
            headers = t0.get('headers', [])
            header_text = html.escape("\n".join(map(str, headers)))
            raw_json = html.escape(json.dumps(lang_obj, ensure_ascii=False, indent=2))
            label_valid   = "Valid" if lang=='en' else "Valide"
            label_err     = "Errors" if lang=='en' else "Erreurs"
            label_rows    = "Rows" if lang=='en' else "Lignes"
            label_headers = "Headers" if lang=='en' else "En-têtes"
            label_raw     = "Raw report JSON" if lang=='en' else "JSON brut du rapport"
            label_result  = "Validation Result" if lang=='en' else "Résultat de la validation"
            ok_text, fail_text = "OK", ("FAIL" if lang=='en' else "Échec")
            meta_list = [
                (label_valid, f'<span class="badge {"ok" if t0.get("valid") else "bad"}">{ok_text if t0.get("valid") else fail_text}</span>'),
                (label_err, str(t0.get("error-count") or 0)),
                (label_rows, str(t0.get("row-count") or "")),
                ("Format", str(t0.get("format") or "")),
                ("Source", html.escape(str(t0.get("source") or ""))),
            ]
            kv = ''.join(f'<div>{k}</div><div>{v}</div>' for k,v in meta_list)
            display = '' if lang=='en' else 'style="display:none"'
            sections.append(f"""
<section data-lang="{lang}" {display}>
  <h2>{label_result}</h2>
  <div class="kv">{kv}</div>
  <h3 style="margin-top:12px">{label_err}</h3>
  {render_errors(errors, lang)}
  <details style="margin-top:12px"><summary>{label_headers}</summary><div class="code">{header_text}</div></details>
  <details style="margin-top:12px"><summary>{label_raw}</summary><div class="code">{raw_json}</div></details>
</section>
""")
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
  {''.join(sections)}
</div></div></body></html>"""
        with open(os.path.join(rdir, f"{pid}.html"), "w", encoding="utf-8") as f:
            f.write(page)

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
