#!/usr/bin/env python3
from __future__ import annotations

import re
import shutil
from pathlib import Path

import build_report
import build_bn_funnel

ROOT = Path(__file__).resolve().parents[1]

# Build the generated site directly into BN-ATI so source and published output
# live in one maintained directory without a second generated docs tree.
build_report.OUT_DIR = ROOT
build_report.OUT_SQLITE = ROOT / "data.sqlite"
build_report.OUT_HTML = ROOT / "index.html"
build_report.TEMPLATE_FILE = ROOT / "templates" / "index.html"
build_report.DOCCLOUD_CACHE = ROOT / "data" / "documentcloud_cache.jsonl"

build_bn_funnel.DOCS = ROOT
build_bn_funnel.DB_PATH = ROOT / "data.sqlite"
build_bn_funnel.OUT_JSON = ROOT / "bn-funnel.json"
build_bn_funnel.SOURCE_JS = ROOT / "templates" / "bn-funnel.js"
build_bn_funnel.OUT_JS = ROOT / "bn-funnel.js"


def inject_ui_overrides() -> None:
    source = ROOT / "templates" / "ui-overrides.css"
    target = ROOT / "ui-overrides.css"
    shutil.copy2(source, target)

    html_path = ROOT / "index.html"
    html = html_path.read_text(encoding="utf-8")
    stylesheet = '    <link rel="stylesheet" href="./ui-overrides.css" />\n'
    if 'href="./ui-overrides.css"' not in html:
        anchor = '    <link rel="stylesheet" href="./assets/app.css" />\n'
        if anchor not in html:
            raise RuntimeError("Could not find app.css link for UI overrides")
        html = html.replace(anchor, anchor + stylesheet, 1)
    html_path.write_text(html, encoding="utf-8")


def inject_solution_banner() -> None:
    """Add the transparency-context banner immediately above the licence section."""
    html_path = ROOT / "index.html"
    html = html_path.read_text(encoding="utf-8")

    if 'id="transparency-context"' in html:
        return

    banner = '''
      <section id="transparency-context" class="pipeline-banner" aria-labelledby="transparency-context-heading">
        <div class="pipeline-hero">
          <figure class="lineage-figure">
            <img
              src="./End2End2Open.png"
              alt="Workflow connecting briefing note titles and numbers, formal ATI requests, completed access to information requests, informal ATI record requests, and repositories and discovery."
            />
          </figure>
          <div class="pipeline-copy">
            <h2 id="transparency-context-heading">What does this help solve? where could this fit?</h2>
            <br/>
            <ol><li>Has anyone heard an ATIP Office say things are slow in the office, they have all the people they need all equiped with the tools they need?</li>
            <li>Have you ever heard someone who submitted an informal re-request through the portal say they ended up getting it within a few minutes and in a convenient way?</li>
            <li>Nothing is faster or more convient than just viewing transparey disclosures online.</li>
            <li>It is a literal impossiblity to produce a greater operational for the GC. This diverts volumne away from costly GC processes with ATIP staff manually emailing back and forth with re-requestors.</li> <li>Or in some cases, taking digital content off of internet connected systems, then mailing it on a (single use) USB Drive in a bubblewrapped envelope to the requestor.<li>
            <li>Here each request produces 0 incremental costs, not even the usage based IT costs as service delivers is voluntary by 3rd party systems, mostly civic tech organizations</li>  
            </ol>
            <p>This tool adds connection to transparency disclosures across Open.Canada.ca, allowinga a night and day improvement in service experience on 1000s of transactions with Canadians.</p> 
          </div>
        </div>
      </section>

'''

    licence_anchor = '      <section class="report-section licence-area"'
    if licence_anchor not in html:
        raise RuntimeError("Could not find licence section anchor for transparency context banner")
    html = html.replace(licence_anchor, banner + licence_anchor, 1)
    html_path.write_text(html, encoding="utf-8")


def refine_generated_markup() -> None:
    """Apply durable UI refinements after all generated sections have been injected."""
    html_path = ROOT / "index.html"
    html = html_path.read_text(encoding="utf-8")

    # Use a plain H1 so the page title always renders even if the web component fails.
    html = html.replace(
        '<gcds-heading tag="h1">BN × ATI Report</gcds-heading>',
        '<h1 class="report-title">BN x ATI OpenLinker</h1>',
        1,
    )

    # Keep the first pipeline paragraph visible; move the second paragraph into
    # a closed native details element labelled "more".
    old_pipeline_copy = '''            <gcds-text>
              The report combines three Open Government datasets, CKAN organization aliases,
              and the persistent DocumentCloud cache. Records are matched by organization,
              ATI request and briefing-note reference before being published in the report.
            </gcds-text>'''
    new_pipeline_copy = '''            <p>
              The report combines three Open Government datasets, CKAN organization aliases,
              and the persistent DocumentCloud cache.
            </p>
            <details class="pipeline-more">
              <summary>more</summary>
              <p>
                Records are matched by organization, ATI request and briefing-note reference
                before being published in the report.
              </p>
            </details>'''
    if old_pipeline_copy in html:
        html = html.replace(old_pipeline_copy, new_pipeline_copy, 1)

    html = html.replace(
        '<gcds-heading tag="h2">Matched records</gcds-heading>',
        '<gcds-heading tag="h2">Transparency Velocity Matrix</gcds-heading>',
        1,
    )

    # Convert the whole BN funnel into an open-by-default details section.
    funnel_pattern = re.compile(
        r'''\s*<section id="bn-funnel-section" class="bn-funnel-section" aria-labelledby="bn-funnel-heading">\s*'''
        r'''<h3 id="bn-funnel-heading">Briefing Note Match Funnel</h3>\s*'''
        r'''<p>\s*This Sankey starts with briefing notes in the selected organization and year,\s*'''
        r'''then shows which BNs were referenced in an ATI summary, which survived weak-ID\s*'''
        r'''review, and whether strong matches had informal requests and/or were found online\.\s*</p>\s*'''
        r'''<div id="bn-funnel-scope" class="bn-funnel-scope">Loading briefing-note funnel…</div>\s*'''
        r'''<div id="bn-funnel-chart" class="bn-funnel-chart" role="img" aria-label="Briefing Note Match Funnel"></div>\s*'''
        r'''<div id="bn-funnel-summary" class="bn-funnel-summary" aria-live="polite"></div>\s*'''
        r'''</section>\s*''',
        re.MULTILINE,
    )
    funnel_replacement = '''
        <details id="bn-funnel-section" class="bn-funnel-section" open>
          <summary class="bn-funnel-toggle"><h3 id="bn-funnel-heading">Briefing Note Match Funnel</h3></summary>
          <div class="bn-funnel-content" aria-labelledby="bn-funnel-heading">
            <p>
              This Sankey starts with briefing notes in the selected organization and year,
              then shows which BNs were referenced in an ATI summary, which survived weak-ID
              review, and whether strong matches had informal requests and/or were found online.
            </p>
            <div id="bn-funnel-scope" class="bn-funnel-scope">Loading briefing-note funnel…</div>
            <div id="bn-funnel-chart" class="bn-funnel-chart" role="img" aria-label="Briefing Note Match Funnel"></div>
            <div id="bn-funnel-summary" class="bn-funnel-summary" aria-live="polite"></div>
          </div>
        </details>

'''
    html, funnel_replacements = funnel_pattern.subn(funnel_replacement, html, count=1)
    if funnel_replacements != 1:
        raise RuntimeError("Could not convert Briefing Note Match Funnel into details element")

    extra_styles = '''
    <style id="openlinker-refinements">
      .report-title {
        margin: 0 0 1rem;
        color: #26374a;
        font-size: clamp(2rem, 4vw, 3rem);
        line-height: 1.15;
      }
      .pipeline-more {
        margin-top: .75rem;
      }
      .pipeline-more summary,
      .bn-funnel-toggle {
        cursor: pointer;
        color: #284f7a;
        font-weight: 700;
      }
      .pipeline-more[open] summary {
        margin-bottom: .5rem;
      }
      .bn-funnel-toggle {
        list-style-position: outside;
        margin-left: 1.1rem;
      }
      .bn-funnel-toggle h3 {
        display: inline;
        margin-left: .35rem !important;
      }
      .bn-funnel-content {
        margin-top: .8rem;
      }
      .lineage-figure img {
        cursor: zoom-in;
      }
      .image-lightbox {
        width: min(94vw, 96rem);
        max-width: none;
        max-height: 92vh;
        padding: 0;
        border: 0;
        border-radius: .5rem;
        background: transparent;
        box-shadow: 0 22px 70px rgba(0, 0, 0, .45);
      }
      .image-lightbox::backdrop {
        background: rgba(15, 23, 42, .78);
      }
      .image-lightbox__inner {
        position: relative;
        display: flex;
        align-items: center;
        justify-content: center;
        max-height: 92vh;
        background: #fff;
        border-radius: .5rem;
        overflow: auto;
      }
      .image-lightbox__image {
        display: block;
        max-width: 100%;
        max-height: 88vh;
        width: auto;
        height: auto;
      }
      .image-lightbox__close {
        position: absolute;
        top: .65rem;
        right: .65rem;
        z-index: 2;
        width: 2.6rem;
        height: 2.6rem;
        border: 1px solid #6b7280;
        border-radius: 50%;
        background: rgba(255, 255, 255, .95);
        color: #111827;
        font-size: 1.5rem;
        line-height: 1;
        cursor: pointer;
      }
    </style>
'''
    if 'id="openlinker-refinements"' not in html:
        html = html.replace('  </head>', extra_styles + '  </head>', 1)

    lightbox_script = '''
    <script id="openlinker-lightbox">
      (() => {
        const images = document.querySelectorAll('.lineage-figure img');
        if (!images.length) return;

        const dialog = document.createElement('dialog');
        dialog.className = 'image-lightbox';
        dialog.setAttribute('aria-label', 'Expanded image');
        dialog.innerHTML = `
          <div class="image-lightbox__inner">
            <button type="button" class="image-lightbox__close" aria-label="Close expanded image">×</button>
            <img class="image-lightbox__image" alt="" />
          </div>`;
        document.body.appendChild(dialog);

        const expanded = dialog.querySelector('.image-lightbox__image');
        const close = dialog.querySelector('.image-lightbox__close');

        images.forEach((image) => {
          image.tabIndex = 0;
          image.setAttribute('role', 'button');
          image.setAttribute('aria-label', `${image.alt || 'Image'} — open larger view`);
          const open = () => {
            expanded.src = image.currentSrc || image.src;
            expanded.alt = image.alt || '';
            dialog.showModal();
            close.focus();
          };
          image.addEventListener('click', open);
          image.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
              event.preventDefault();
              open();
            }
          });
        });

        close.addEventListener('click', () => dialog.close());
        dialog.addEventListener('click', (event) => {
          if (event.target === dialog) dialog.close();
        });
      })();
    </script>
'''
    if 'id="openlinker-lightbox"' not in html:
        html = html.replace('  </body>', lightbox_script + '  </body>', 1)

    html_path.write_text(html, encoding="utf-8")


def main() -> None:
    build_report.main()
    build_bn_funnel.main()
    inject_ui_overrides()
    inject_solution_banner()
    refine_generated_markup()
    print(f"Built consolidated site in {ROOT}", flush=True)


if __name__ == "__main__":
    main()
