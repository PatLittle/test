
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
