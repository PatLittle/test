const chartElement = document.getElementById("bn-funnel-chart");
const scopeElement = document.getElementById("bn-funnel-scope");
const summaryElement = document.getElementById("bn-funnel-summary");
const organizationFacet = document.getElementById("facet-organization");
const yearFacet = document.getElementById("facet-year");
const clearFacetButton = document.getElementById("facet-clear");

const numericFields = [
  "all_bns",
  "referenced",
  "not_referenced",
  "strong",
  "weak",
  "strong_req_online",
  "strong_req_not_online",
  "strong_no_req_online",
  "strong_no_req_not_online",
];

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString("en-CA");
}

function percentage(value, denominator) {
  if (!denominator) return "0.0%";
  return `${((Number(value || 0) / denominator) * 100).toFixed(1)}%`;
}

async function loadData() {
  const url = new URL("./bn-funnel.json", window.location.href);
  url.searchParams.set("v", Date.now().toString());
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`BN funnel data request failed: HTTP ${response.status}`);
  const payload = await response.json();
  return Array.isArray(payload.rows) ? payload.rows : [];
}

function aggregateRows(rows) {
  const organization = organizationFacet?.value || "";
  const year = yearFacet?.value || "";
  const selected = rows.filter((row) =>
    (!organization || row.owner_org === organization) &&
    (!year || String(row.briefing_note_year || "") === year)
  );

  const totals = Object.fromEntries(numericFields.map((field) => [field, 0]));
  for (const row of selected) {
    for (const field of numericFields) totals[field] += Number(row[field] || 0);
  }
  return { totals, selected, organization, year };
}

function selectedScopeLabel(organization, year) {
  const organizationLabel = organization
    ? organizationFacet?.selectedOptions?.[0]?.textContent?.replace(/\s*\([\d,]+\)\s*$/, "") || organization
    : "All organizations";
  const yearLabel = year || "All briefing-note years";
  return `${organizationLabel} · ${yearLabel}`;
}

function svgElement(name, attributes = {}, text = "") {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, String(value));
  if (text) element.textContent = text;
  return element;
}

function drawLabel(svg, node, total) {
  const x = node.x + 26;
  const y = node.y + Math.min(Math.max(node.h / 2 - 8, 5), 28);
  const text = svgElement("text", {
    x,
    y,
    fill: "#26374a",
    "font-family": "Arial, sans-serif",
    "font-size": "15",
    "font-weight": "700",
  });
  const title = svgElement("tspan", { x, dy: "0" }, node.label);
  const count = svgElement("tspan", {
    x,
    dy: "20",
    fill: "#52606d",
    "font-size": "13",
    "font-weight": "600",
  }, `${formatNumber(node.value)} · ${percentage(node.value, total)} of all BNs`);
  text.append(title, count);
  svg.appendChild(text);
}

function renderSankey(totals) {
  if (!chartElement) return;
  chartElement.replaceChildren();

  const total = totals.all_bns;
  if (!total) {
    chartElement.innerHTML = '<p>No briefing notes are available for this organization/year selection.</p>';
    return;
  }

  const width = 1200;
  const height = 680;
  const nodeWidth = 18;
  const top = 82;
  const usable = 350;
  const gap = 28;
  const outcomeGap = 68;
  const scale = usable / total;
  const xs = [45, 330, 625, 920];

  const nodeHeight = (value) => value > 0 ? Math.max(8, value * scale) : 0;
  const nodes = {
    all: { x: xs[0], y: top + 35, h: nodeHeight(total), value: total, label: "All Briefing Notes", color: "#2b6cb0" },
    referenced: { x: xs[1], y: top, h: nodeHeight(totals.referenced), value: totals.referenced, label: "Referenced in ATI summary", color: "#4c7c6b" },
    notReferenced: { x: xs[1], y: top + nodeHeight(totals.referenced) + gap, h: nodeHeight(totals.not_referenced), value: totals.not_referenced, label: "Not referenced in ATI summary", color: "#8c96a3" },
    strong: { x: xs[2], y: top + 15, h: nodeHeight(totals.strong), value: totals.strong, label: "Strong matches", color: "#2f855a" },
    weak: { x: xs[2], y: top + 15 + nodeHeight(totals.strong) + gap, h: nodeHeight(totals.weak), value: totals.weak, label: "Weak IDs separated", color: "#b42318" },
  };

  const outcomeValues = [
    ["reqOnline", totals.strong_req_online, "Informal requests + online", "#2878a5"],
    ["reqNotOnline", totals.strong_req_not_online, "Informal requests + not online", "#7b61a8"],
    ["noReqOnline", totals.strong_no_req_online, "No informal requests + online", "#5b8f67"],
    ["noReqNotOnline", totals.strong_no_req_not_online, "No informal requests + not online", "#7a8793"],
  ];
  let outcomeY = top - 8;
  for (const [key, value, label, color] of outcomeValues) {
    const h = nodeHeight(value);
    nodes[key] = { x: xs[3], y: outcomeY, h, value, label, color };
    if (value > 0) outcomeY += Math.max(h, 18) + outcomeGap;
  }

  const svg = svgElement("svg", {
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-labelledby": "bn-sankey-title bn-sankey-desc",
    preserveAspectRatio: "xMidYMid meet",
  });
  svg.appendChild(svgElement("title", { id: "bn-sankey-title" }, "Briefing Note Match Funnel"));
  svg.appendChild(svgElement("desc", { id: "bn-sankey-desc" }, "Sankey showing how briefing notes narrow to ATI references, strong and weak matches, and strong-match request and online outcomes."));

  const flowLayer = svgElement("g", { fill: "none", "stroke-linecap": "butt" });
  const nodeLayer = svgElement("g");
  svg.append(flowLayer, nodeLayer);

  function addLink(source, target, value, sourceOffset, targetOffset, color) {
    if (!value) return;
    const thickness = Math.max(2, value * scale);
    const sy = source.y + sourceOffset * scale + thickness / 2;
    const ty = target.y + targetOffset * scale + thickness / 2;
    const sx = source.x + nodeWidth;
    const tx = target.x;
    const control = (tx - sx) * 0.45;
    const path = svgElement("path", {
      d: `M ${sx} ${sy} C ${sx + control} ${sy}, ${tx - control} ${ty}, ${tx} ${ty}`,
      stroke: color,
      "stroke-width": thickness,
      opacity: "0.32",
    });
    path.appendChild(svgElement("title", {}, `${source.label} → ${target.label}: ${formatNumber(value)}`));
    flowLayer.appendChild(path);
  }

  addLink(nodes.all, nodes.referenced, totals.referenced, 0, 0, nodes.referenced.color);
  addLink(nodes.all, nodes.notReferenced, totals.not_referenced, totals.referenced, 0, nodes.notReferenced.color);
  addLink(nodes.referenced, nodes.strong, totals.strong, 0, 0, nodes.strong.color);
  addLink(nodes.referenced, nodes.weak, totals.weak, totals.strong, 0, nodes.weak.color);

  let strongOffset = 0;
  for (const [key, value] of outcomeValues) {
    addLink(nodes.strong, nodes[key], value, strongOffset, 0, nodes[key].color);
    strongOffset += value;
  }

  for (const node of Object.values(nodes)) {
    if (!node.value) continue;
    nodeLayer.appendChild(svgElement("rect", {
      x: node.x,
      y: node.y,
      width: nodeWidth,
      height: Math.max(node.h, 8),
      rx: "3",
      fill: node.color,
    }));
    drawLabel(nodeLayer, node, total);
  }

  chartElement.appendChild(svg);
}

function renderSummary(totals) {
  if (!summaryElement) return;
  summaryElement.innerHTML = [
    ["All BNs", totals.all_bns],
    ["Referenced", totals.referenced],
    ["Strong", totals.strong],
    ["Weak", totals.weak],
    ["Strong with informal requests", totals.strong_req_online + totals.strong_req_not_online],
    ["Strong found online", totals.strong_req_online + totals.strong_no_req_online],
  ].map(([label, value]) =>
    `<span><strong>${escapeHtml(label)}:</strong> ${formatNumber(value)} (${percentage(value, totals.all_bns)})</span>`
  ).join("");
}

let rows = [];

function render() {
  const { totals, organization, year } = aggregateRows(rows);
  if (scopeElement) scopeElement.textContent = selectedScopeLabel(organization, year);
  renderSankey(totals);
  renderSummary(totals);
}

function moveFacetsAboveFunnel() {
  const funnelSection = document.getElementById("bn-funnel-section");
  const facets = document.querySelector(".table-facets");
  if (!funnelSection || !facets || funnelSection.previousElementSibling === facets) return;
  funnelSection.parentNode.insertBefore(facets, funnelSection);
}

async function initialize() {
  if (!chartElement) return;
  moveFacetsAboveFunnel();
  try {
    rows = await loadData();
    render();
  } catch (error) {
    console.error("BN funnel failed", error);
    if (scopeElement) scopeElement.textContent = "Briefing-note funnel could not be loaded.";
    chartElement.innerHTML = `<p>${escapeHtml(error.message || error)}</p>`;
  }
}

organizationFacet?.addEventListener("change", render);
yearFacet?.addEventListener("change", render);
clearFacetButton?.addEventListener("click", () => setTimeout(render, 0));

initialize();
