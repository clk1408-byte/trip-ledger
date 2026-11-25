/* Jetsetr — offline-first trip planner + cost splitter, synced through a private GitHub repository. */
{
const $=s=>document.querySelector(s);
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const uid=p=>p+Date.now().toString(36)+Math.random().toString(36).slice(2,8);
const APP_VERSION='1.0.4';

/* ================= local storage (IndexedDB) ================= */
const IDB={db:null,
  open(){return this.db?Promise.resolve(this.db):new Promise((res,rej)=>{const r=indexedDB.open('tripledger',1);r.onupgradeneeded=()=>{const d=r.result;if(!d.objectStoreNames.contains('kv'))d.createObjectStore('kv');if(!d.objectStoreNames.contains('blobs'))d.createObjectStore('blobs')};r.onsuccess=()=>{this.db=r.result;res(this.db)};r.onerror=()=>rej(r.error)})},
  async get(store,k){const d=await this.open();return new Promise((res,rej)=>{const t=d.transaction(store).objectStore(store).get(k);t.onsuccess=()=>res(t.result);t.onerror=()=>rej(t.error)})},
  async set(store,k,v){const d=await this.open();return new Promise((res,rej)=>{const tx=d.transaction(store,'readwrite');tx.objectStore(store).put(v,k);tx.oncomplete=()=>res();tx.onerror=()=>rej(tx.error)})},
  async del(store,k){const d=await this.open();return new Promise((res,rej)=>{const tx=d.transaction(store,'readwrite');tx.objectStore(store).delete(k);tx.oncomplete=()=>res();tx.onerror=()=>rej(tx.error)})},
  async clear(){const d=await this.open();return new Promise((res,rej)=>{const tx=d.transaction(['kv','blobs'],'readwrite');tx.objectStore('kv').clear();tx.objectStore('blobs').clear();tx.oncomplete=()=>res();tx.onerror=()=>rej(tx.error)})}};
const LS={get:k=>{try{return JSON.parse(localStorage.getItem(k))}catch(e){return null}},set:(k,v)=>{try{localStorage.setItem(k,JSON.stringify(v))}catch(e){}},del:k=>{try{localStorage.removeItem(k)}catch(e){}}};

/* state: files = what we last saw on GitHub {path:{sha,data}}, pending = local changes not yet on GitHub {path:{op,data,ts}} */
const S={cfg:LS.get('tl_cfg'),files:{},pending:{},upQueue:{},remoteBlobs:{},headSha:null,trips:new Map(),items:new Map(),vault:null,
  sync:{state:'idle',at:LS.get('tl_lastsync'),msg:''},urls:{}};
let saveT=null;const persist=()=>{clearTimeout(saveT);saveT=setTimeout(async()=>{try{await IDB.set('kv','state',{files:S.files,pending:S.pending,upQueue:S.upQueue,remoteBlobs:S.remoteBlobs,headSha:S.headSha})}catch(e){toast('Couldn’t save on this phone: '+e.message)}},150)};
function rebuild(){const view={};for(const[p,f]of Object.entries(S.files))view[p]=f.data;for(const[p,c]of Object.entries(S.pending)){if(c.op==='del')delete view[p];else view[p]=c.data}
  S.trips=new Map();S.items=new Map();S.vault=null;
  for(const[p,d]of Object.entries(view)){if(!d)continue;let m;if(m=/^trips\/([\w-]+)\.json$/.exec(p))S.trips.set(m[1],Object.assign({},d,{id:m[1]}));else if(m=/^items\/([\w-]+)\.json$/.exec(p))S.items.set(m[1],Object.assign({},d,{id:m[1]}));else if(p==='vault.json')S.vault=d}}
const me=()=>(S.cfg&&S.cfg.me)||'a';
function change(path,data){S.pending[path]=data===null?{op:'del',ts:Date.now()}:{op:'put',data,ts:Date.now()};persist();rebuild();route();queueSync(2500)}
function putItem(it){const id=it.id;const d=clean(it);delete d.id;d.updatedAt=Date.now();d.by=me();change('items/'+id+'.json',d)}
function putTrip(t){const id=t.id;const d=clean(t);delete d.id;d.updatedAt=Date.now();d.by=me();change('trips/'+id+'.json',d)}
function delItem(id){change('items/'+id+'.json',null)}
function delTrip(id){for(const it of tripItems(id))S.pending['items/'+it.id+'.json']={op:'del',ts:Date.now()};change('trips/'+id+'.json',null)}

/* ================= GitHub sync ================= */
const b64enc=bytes=>{let b='';for(let i=0;i<bytes.length;i+=0x8000)b+=String.fromCharCode.apply(null,bytes.subarray(i,i+0x8000));return btoa(b)};
const b64dec=s=>{const b=atob(String(s).replace(/\s/g,''));const u=new Uint8Array(b.length);for(let i=0;i<b.length;i++)u[i]=b.charCodeAt(i);return u};
const te=new TextEncoder(),td=new TextDecoder();
async function gh(path,opt={}){const c=S.cfg;const r=await fetch('https://api.github.com'+path,{method:opt.method||'GET',cache:'no-store',headers:Object.assign({'Accept':'application/vnd.github+json','Authorization':'Bearer '+c.token,'X-GitHub-Api-Version':'2022-11-28'},opt.body?{'Content-Type':'application/json'}:{},opt.etag?{'If-None-Match':opt.etag}:{}),body:opt.body?JSON.stringify(opt.body):undefined});
  if(r.status===304)return{status:304};let j=null;try{j=await r.json()}catch(e){}if(!r.ok){const e=new Error((j&&j.message)||('HTTP '+r.status));e.status=r.status;throw e}return{status:r.status,json:j,etag:r.headers.get('ETag')}}
const R=()=>`/repos/${encodeURIComponent(S.cfg.owner)}/${encodeURIComponent(S.cfg.repo)}`;
const encPath=p=>p.split('/').map(encodeURIComponent).join('/');
let syncing=false,syncTimer=null,again=false;
function queueSync(ms){clearTimeout(syncTimer);syncTimer=setTimeout(sync,ms)}
function setSync(state,msg){S.sync.state=state;S.sync.msg=msg||'';if(state==='ok'){S.sync.at=Date.now();LS.set('tl_lastsync',S.sync.at)}drawSync()}
async function sync(force){if(!S.cfg||!S.cfg.token)return;if(syncing){again=true;return}if(!navigator.onLine){setSync('offline');return}
  syncing=true;setSync('busy');
  try{
    const br=S.cfg.branch||'main';
    const head=await gh(`${R()}/branches/${encodeURIComponent(br)}`,{etag:force?null:S.etag});
    let changedRemote=false;
    if(head.status!==304){S.etag=head.etag;const sha=head.json.commit.sha;if(sha!==S.headSha){changedRemote=true;await pull(head.json.commit.commit.tree.sha);S.headSha=sha}}
    const pushed=await push();
    await pullBlobs();
    if(pushed)again=true;
    persist();rebuild();if(changedRemote||pushed)route();
    const waiting=Object.keys(S.pending).length+Object.keys(S.upQueue).length;setSync(waiting?'wait':'ok',waiting?waiting+' change'+(waiting>1?'s':'')+' waiting':'')}
  catch(e){const st=e.status;setSync('err',st===401?'Access key rejected or expired':st===404?'Can’t find the data repository (check the key’s access)':st===403?'GitHub refused: '+e.message:(e.message||'Sync failed'));}
  finally{syncing=false;if(again){again=false;queueSync(800)}}}
async function pull(treeSha){const t=await gh(`${R()}/git/trees/${treeSha}?recursive=1`);const remote={};const blobs={};
  for(const n of t.json.tree){if(n.type!=='blob')continue;if(/^(trips|items)\/[\w-]+\.json$/.test(n.path)||n.path==='vault.json')remote[n.path]=n.sha;else if(/^attachments\/[\w-]+$/.test(n.path))blobs[n.path.slice(12)]=n.sha}
  S.remoteBlobs=blobs;
  for(const[p,sha]of Object.entries(remote)){const loc=S.files[p];if(loc&&loc.sha===sha)continue;
    const b=await gh(`${R()}/git/blobs/${sha}`);let data=null;try{data=JSON.parse(td.decode(b64dec(b.json.content)))}catch(e){continue}
    S.files[p]={sha,data};const pend=S.pending[p];
    if(pend){const rts=(data&&data.updatedAt)||0;if(rts>=pend.ts)delete S.pending[p]}}
  for(const p of Object.keys(S.files))if(!remote[p]){delete S.files[p];if(S.pending[p]&&S.pending[p].op==='del')delete S.pending[p]}}
async function push(){let did=false;
  for(const[p,c]of Object.entries(S.pending)){const known=S.files[p];
    try{
      if(c.op==='del'){if(known){await gh(`${R()}/contents/${encPath(p)}`,{method:'DELETE',body:{message:`${who()}: remove ${p}`,sha:known.sha}});delete S.files[p]}delete S.pending[p];did=true;continue}
      const body={message:`${who()}: ${known?'update':'add'} ${p}`,content:b64enc(te.encode(JSON.stringify(c.data,null,1)))};if(known)body.sha=known.sha;
      const r=await gh(`${R()}/contents/${encPath(p)}`,{method:'PUT',body});S.files[p]={sha:r.json.content.sha,data:c.data};
      if(S.pending[p]===c)delete S.pending[p];did=true}
    catch(e){if(e.status===409||e.status===422){/* someone changed it first: take theirs if newer, else retry on top */
        const cur=await gh(`${R()}/contents/${encPath(p)}`).catch(()=>null);
        if(cur&&cur.json&&cur.json.sha){let data=null;try{data=JSON.parse(td.decode(b64dec(cur.json.content)))}catch(x){}S.files[p]={sha:cur.json.sha,data};if(data&&(data.updatedAt||0)>=c.ts)delete S.pending[p]}
        else if(!cur)delete S.files[p];again=true}
      else throw e}}
  for(const id of Object.keys(S.upQueue)){const rec=await IDB.get('blobs',id);if(!rec){delete S.upQueue[id];continue}
    const body={message:`${who()}: attach file`,content:b64enc(new Uint8Array(rec.buf))};if(S.remoteBlobs[id])body.sha=S.remoteBlobs[id];
    const r=await gh(`${R()}/contents/attachments/${encodeURIComponent(id)}`,{method:'PUT',body});S.remoteBlobs[id]=r.json.content.sha;delete S.upQueue[id];did=true}
  return did}
async function pullBlobs(){/* keep every attachment on the phone for offline use */
  for(const id of Object.keys(S.remoteBlobs)){if(!S.remoteBlobs[id])continue;const have=await IDB.get('blobs',id);if(have&&have.sha===S.remoteBlobs[id])continue;
    try{const b=await gh(`${R()}/git/blobs/${S.remoteBlobs[id]}`);const bytes=b64dec(b.json.content);const meta=attMeta(id);await IDB.set('blobs',id,{buf:bytes.buffer,type:meta.type||'application/octet-stream',name:meta.name||id,sha:S.remoteBlobs[id]})}catch(e){}}}
function attMeta(id){for(const it of S.items.values())for(const a of it.attachments||[])if(a.id===id)return a;for(const t of S.trips.values())if(t.photo===id)return{type:t.photoType||'image/jpeg',name:'cover'};return{}}
async function addBlob(file){const id=uid('f');const buf=await file.arrayBuffer();await IDB.set('blobs',id,{buf,type:file.type||'application/octet-stream',name:file.name||id});S.upQueue[id]=true;persist();return id}
async function blobURL(id){if(S.urls[id])return S.urls[id];const rec=await IDB.get('blobs',id);if(!rec)return null;const u=URL.createObjectURL(new Blob([rec.buf],{type:rec.type}));S.urls[id]=u;return u}
async function hydrate(root=document){for(const img of root.querySelectorAll('img[data-att]')){const u=await blobURL(img.dataset.att);if(u)img.src=u;else img.style.visibility='hidden'}
  for(const a of root.querySelectorAll('a[data-att]')){const u=await blobURL(a.dataset.att);if(u){a.href=u;a.removeAttribute('aria-disabled')}else{a.textContent+=' (downloads next time you’re online)';a.removeAttribute('href')}}}
const who=()=>pname(null,me());
window.addEventListener('online',()=>queueSync(300));window.addEventListener('offline',()=>setSync('offline'));
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')queueSync(300)});
setInterval(()=>{if(document.visibilityState==='visible')sync()},4*60*1000);

/* ================= shared helpers ================= */
const CURS=['CAD','USD','EUR','MXN','GBP','AUD','NZD','JPY','CHF','DKK','SEK','NOK','ISK','CZK','HUF','PLN','TRY','EGP','MAD','ZAR','THB','VND','IDR','SGD','HKD','CNY','KRW','INR','AED','BRL','ARS','CLP','COP','PEN','CRC','DOP','XCD','JMD','BSD','BBD','XPF','FJD'];
const ICONS={flight:'M21 15.5v-2l-8-5V3.6a1.5 1.5 0 0 0-3 0v4.9l-8 5v2l8-2.5v5l-2 1.5V21l3.5-1 3.5 1v-1.5l-2-1.5v-5z',hotel:'M3 18V6M3 14h18v4M21 14v-2a3 3 0 0 0-3-3h-7v5M7 12.2a1.7 1.7 0 1 0 0-.01',car:'M5 16h14M6 16l1.6-5.2h8.8L18 16M5 16v3M19 16v3M8.5 13.5h.01M15.5 13.5h.01',train:'M7 4h10a2 2 0 0 1 2 2v9a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3V6a2 2 0 0 1 2-2zM5 11h14M8 21l2-3M16 21l-2-3',restaurant:'M7 3v8M5 3v5a2 2 0 0 0 4 0V3M7 11v10M16 3c-2 1-3 4-3 7h3v11',activity:'M12 3l2.6 5.6 6 .7-4.5 4.1 1.2 6L12 16.5 6.7 19.4l1.2-6L3.4 9.3l6-.7z',run:'M13 4.5a1.5 1.5 0 1 0 0-.01M9 20l3-6 3 2v5M6 12l3-4 4 1 3 4 3 1',expense:'M6 3h12v18l-3-2-3 2-3-2-3 2zM9 8h6M9 12h6',other:'M12 21s-6-5.5-6-11a6 6 0 0 1 12 0c0 5.5-6 11-6 11zM12 10h.01',settle:'M4 12h16M14 6l6 6-6 6',clock:'M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM12 7v5l3 2',lock:'M6 11h12v10H6zM8.5 11V8a3.5 3.5 0 0 1 7 0v3',finger:'M7 19c1-2 1.5-4.5 1.5-7a3.5 3.5 0 0 1 7 0c0 1.5-.2 3-.6 4.5M11.5 21c.8-2.5 1.2-5.5 1.2-9M5 15c.3-1.2.5-2.3.5-3.5a6.5 6.5 0 0 1 13 0c0 2.6-.4 5-1.3 7.5M4.8 8.5A8 8 0 0 1 12 3.5'};
const TYPES={flight:'Flight',hotel:'Hotel',car:'Car rental',train:'Train / bus',restaurant:'Meal',activity:'Activity',run:'Run',expense:'Expense',other:'Note',settle:'Payment'};
const CATS={food:'Food & drink',transport:'Transport',lodging:'Lodging',activity:'Activities',shopping:'Shopping',other:'Other'};
const typeCat={flight:'transport',train:'transport',car:'transport',hotel:'lodging',restaurant:'food',activity:'activity',run:'activity',other:'other'};
const SPLITS=[['half','Split 50/50'],['mine','Payer’s own'],['theirs','All for the other'],['custom','Custom']];
const svgI=(k,w=2)=>`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round"><path d="${ICONS[k]||ICONS.other}"/></svg>`;
const dot=t=>`<div class="dot">${svgI(t)}</div>`;
const toast=(m,ms=3200)=>{const t=$('#toast');t.textContent=m;t.classList.add('on');clearTimeout(toast.h);toast.h=setTimeout(()=>t.classList.remove('on'),ms)};
const money=(n,c='CAD')=>{if(n==null||isNaN(n))return '—';try{return new Intl.NumberFormat('en-CA',{style:'currency',currency:c,currencyDisplay:c==='CAD'?'narrowSymbol':'code'}).format(n).replace(/^(-?)\$/,'$1C$')}catch(e){return c+' '+(+n).toFixed(2)}};
const cadStr=n=>money(n,'CAD');
const splitDT=s=>{s=String(s||'');return{d:s.slice(0,10),t:s.length>10?s.slice(11,16):''}};
const dObj=d=>{const[y,m,dd]=String(d).split('-').map(Number);return new Date(y,(m||1)-1,dd||1)};
const dayLbl=(d,o={weekday:'short',month:'short',day:'numeric'})=>/^\d{4}-\d\d-\d\d$/.test(d)?dObj(d).toLocaleDateString('en-US',o):'Undated';
const addDays=(d,n)=>{const x=dObj(d);x.setDate(x.getDate()+n);return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0')};
const today=()=>{const x=new Date();return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0')};
const httpsOnly=u=>/^https:\/\//i.test(String(u||''))?String(u):'';
const DEFAULT_PEOPLE=[{id:'a',name:'Christian'},{id:'b',name:'Tami'}];
const people=tr=>(tr&&tr.people&&tr.people.length?tr.people:DEFAULT_PEOPLE);
const solo=tr=>people(tr).length<2;
const pname=(tr,id)=>(people(tr).find(p=>p.id===id)||{}).name||'Not set';
const tripItems=id=>[...S.items.values()].filter(i=>i.tripId===id);
const mins=s=>{const{d,t}=splitDT(s);if(!d||!t)return null;const[h,m]=t.split(':').map(Number);return dObj(d).getTime()/6e4+h*60+m};
const dur=m=>{const h=Math.floor(m/60),r=m%60;return (h?h+'h':'')+(r?String(r).padStart(h?2:1,'0')+'m':'')||'0m'};
const hasCost=it=>it.cost&&it.cost.amount!=null&&it.cost.amount!=='';
const clean=o=>JSON.parse(JSON.stringify(o));
const ago=t=>{if(!t)return 'never';const s=(Date.now()-t)/1000;return s<60?'just now':s<3600?Math.round(s/60)+' min ago':s<86400?Math.round(s/3600)+' h ago':new Date(t).toLocaleDateString('en-CA',{month:'short',day:'numeric'})};

/* ================= destination covers ================= */
function hashN(s){let h=2166136261;for(const c of String(s))h=Math.imul(h^c.charCodeAt(0),16777619);return h>>>0}
const ART={
 azores:()=>`<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice"><rect width="200" height="200" fill="#bfe2f7"/><path d="M-10 92 C30 58 70 60 100 66 C135 58 170 56 210 88 L210 200 L-10 200Z" fill="#2f6d3c"/><ellipse cx="100" cy="112" rx="92" ry="26" fill="#27552f"/><path d="M14 112 C30 98 70 96 98 104 C100 116 92 126 70 130 C40 132 18 124 14 112Z" fill="#2581d0"/><path d="M102 104 C130 96 168 98 186 112 C180 124 160 132 130 130 C110 128 100 118 102 104Z" fill="#46ad74"/><path d="M-10 150 C40 128 90 140 120 146 C150 140 180 134 210 142 L210 200 L-10 200Z" fill="#4f9c52"/></svg>`,
 mexico:()=>`<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice"><rect width="200" height="200" fill="#f08a5d"/><path d="M40 110 L92 42 L104 40 L160 110Z" fill="#6c5a8a"/><rect y="108" width="200" height="92" fill="#7a3d2e"/><g fill="#c8613f"><rect x="34" y="78" width="30" height="80"/><rect x="136" y="78" width="30" height="80"/><rect x="60" y="104" width="80" height="54"/></g><rect y="158" width="200" height="42" fill="#f6f1e6"/></svg>`,
 gen:(name)=>{const h=hashN(name),hue=h%360;return `<svg viewBox="0 0 200 200" preserveAspectRatio="xMidYMid slice"><rect width="200" height="200" fill="hsl(${hue} 60% 62%)"/><circle cx="${60+h%80}" cy="78" r="22" fill="hsl(${(hue+60)%360} 95% 88%)"/><path d="M-10 132 L40 80 L80 120 L120 70 L210 140 L210 200 L-10 200Z" fill="hsl(${hue} 35% 38%)"/><rect y="160" width="200" height="40" fill="hsl(${(hue+190)%360} 55% 40%)"/></svg>`}};
const artKey=tr=>{const n=(tr.name||'');return /azores|açores|são miguel|sao miguel/i.test(n)?'azores':/mexic|puebla|oaxaca|cdmx|merida|cancun|tulum/i.test(n)?'mexico':'gen'};
function thumb(tr,cls='th'){if(tr.photo)return `<span class="${cls}"><img alt="" data-att="${esc(tr.photo)}"></span>`;
  if(tr.cover&&httpsOnly(tr.cover.thumb))return `<span class="${cls}"><img alt="" src="${esc(tr.cover.thumb)}" onerror="this.parentNode.innerHTML=window.__art(${esc(JSON.stringify(tr.name||''))})"><span class="credit">${esc(tr.cover.credit||'Wikimedia Commons')}</span></span>`;
  return `<span class="${cls}" aria-hidden="true">${ART[artKey(tr)](tr.name)}</span>`}
window.__art=n=>ART[artKey({name:n})](n);
async function findCover(name,skip=0){/* free, credited photos from Wikimedia Commons */
  const q=encodeURIComponent(name+' landscape');const u=`https://commons.wikimedia.org/w/api.php?action=query&format=json&origin=*&generator=search&gsrnamespace=6&gsrlimit=12&gsrsearch=${q}%20filetype:bitmap&prop=imageinfo&iiprop=url|extmetadata|size|mime&iiurlwidth=640`;
  const r=await fetch(u);const j=await r.json();const pages=Object.values((j.query&&j.query.pages)||{}).sort((a,b)=>a.index-b.index);
  const ok=pages.filter(p=>{const i=p.imageinfo&&p.imageinfo[0];return i&&/jpeg|png/.test(i.mime)&&i.width>=800&&i.width>=i.height});
  const p=ok[skip%Math.max(1,ok.length)];if(!p)return null;const i=p.imageinfo[0],md=i.extmetadata||{};
  const strip=h=>{const d=document.createElement('div');d.innerHTML=String(h||'');return d.textContent.trim()};
  return{thumb:i.thumburl,page:i.descriptionurl,credit:(strip(md.Artist&&md.Artist.value)||'Wikimedia Commons').slice(0,60)+(md.LicenseShortName?' · '+strip(md.LicenseShortName.value):''),idx:skip}}

/* ================= money ================= */
function cadOf(it,tr){const c=it.cost;if(!hasCost(it))return 0;if(c.cad!=null&&c.cad!=='')return +c.cad;const cur=c.currency||'CAD';if(cur==='CAD')return +c.amount;const r=tr&&tr.rates&&tr.rates[cur];return r?+c.amount*+r:null}
function sharesOf(it,tr,cad){const ps=people(tr).map(p=>p.id),c=it.cost||{},out={},payer=c.paidBy;ps.forEach(p=>out[p]=0);const mode=c.split||'half';
  if(mode==='mine'&&payer)out[payer]=cad;else if(mode==='theirs'&&payer){const o=ps.filter(p=>p!==payer);o.forEach(p=>out[p]=cad/o.length)}
  else if(mode==='custom'&&c.custom){const tot=ps.reduce((s,p)=>s+(+c.custom[p]||0),0);if(tot>0)ps.forEach(p=>out[p]=cad*(+c.custom[p]||0)/tot);else ps.forEach(p=>out[p]=cad/ps.length)}
  else ps.forEach(p=>out[p]=cad/ps.length);return out}
function ledger(tr){const ps=people(tr).map(p=>p.id),L={total:0,paid:{},share:{},net:{},missing:[],unassigned:[],cats:{}};ps.forEach(p=>{L.paid[p]=0;L.share[p]=0;L.net[p]=0});
  for(const it of tripItems(tr.id)){if(!hasCost(it))continue;const c=it.cost,cad=cadOf(it,tr);if(cad==null){L.missing.push(it);continue}
    const settle=it.type==='settle';if(!settle){L.total+=cad;const k=it.category||typeCat[it.type]||'other';L.cats[k]=(L.cats[k]||0)+cad}
    const payer=ps.length<2?ps[0]:c.paidBy;if(!payer){if(!settle)L.unassigned.push(it);continue}
    const sh=sharesOf(Object.assign({},it,{cost:Object.assign({},c,{paidBy:payer})}),tr,cad);if(!settle){L.paid[payer]=(L.paid[payer]||0)+cad;ps.forEach(p=>L.share[p]+=sh[p])}
    L.net[payer]=(L.net[payer]||0)+cad;ps.forEach(p=>L.net[p]-=sh[p])}
  return L}
function oweLine(tr,L){const ps=people(tr);if(ps.length<2)return{solo:true};const n=L.net[ps[0].id];if(Math.abs(n)<0.005)return{even:true};return n>0?{from:ps[1],to:ps[0],amt:n}:{from:ps[0],to:ps[1],amt:-n}}

/* ================= routing & top bar ================= */
function route(){if(!S.cfg)return renderSetup();const h=location.hash.slice(1);const m=/^(t|m)-([\w]+)$/.exec(h);
  if(h==='vault')return renderVault();if(m&&S.trips.has(m[2]))return renderTrip(m[2],m[1]==='m'?'money':'plan');renderTrips()}
window.addEventListener('hashchange',()=>{closeSheet();route();scrollTo(0,0)});
const BACK=`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 12H5M11 6l-6 6 6 6"/></svg>`;
function syncBtn(){return `<button class="sync" id="syncBtn"></button>`}
function drawSync(){const b=$('#syncBtn');if(!b)return;const s=S.sync,n=Object.keys(S.pending).length+Object.keys(S.upQueue).length;
  const st=s.state==='busy'?['wait','Syncing…']:s.state==='err'?['err',s.msg||'Sync problem']:!navigator.onLine||s.state==='offline'?['wait',n?`Offline · ${n} to sync`:'Offline']:n?['wait',`${n} to sync`]:['ok','Synced '+ago(s.at)];
  b.className='sync '+st[0];b.innerHTML=`<i></i>${esc(st[1])}`;b.onclick=settingsSheet}

/* ================= trips list ================= */
function renderTrips(){
  $('#top').innerHTML=`${syncBtn()}<div class="sp"></div><button class="pill" id="vaultTop">${svgI('lock',2)} Vault</button>`;drawSync();
  const vb=$('#vaultTop');vb.style.display='inline-flex';vb.style.alignItems='center';vb.style.gap='6px';vb.querySelector('svg').style.width='16px';vb.onclick=()=>location.hash='vault';
  const trips=[...S.trips.values()].sort((a,b)=>(a.start||'').localeCompare(b.start||''));const now=today();
  const up=trips.filter(t=>(t.end||t.start||'9')>=now),past=trips.filter(t=>(t.end||t.start||'9')<now).reverse();
  $('#head').innerHTML=`<h1>Trips</h1>${!trips.length?`<p class="lede">${S.headSha?'No trips yet. Tap + to add one, or ask Claude to add your bookings.':'Getting your trips from GitHub…'}</p>`:!up.length?'<p class="lede">No upcoming trips.</p>':''}`;
  const row=t=>{const L=ledger(t),o=oweLine(t,L),n=t.start&&t.end?Math.round((dObj(t.end)-dObj(t.start))/864e5)+1:null,away=t.start?Math.round((dObj(t.start)-dObj(now))/864e5):null;
    return `<button class="trow" data-trip="${esc(t.id)}">${thumb(t)}<span><b>${esc(t.name)}</b><span class="d">${esc(dayLbl(t.start))} - ${esc(dayLbl(t.end,{weekday:'short',month:'short',day:'numeric',year:'numeric'}))}</span>
      <span class="m">${svgI('clock',1.8)}${n?n+' days':''}${away>0?` · in ${away} day${away===1?'':'s'}`:''}</span>${o.from?`<span class="o">${esc(o.from.name)} owes ${esc(o.to.name)} ${cadStr(o.amt)}</span>`:''}</span></button>`};
  $('#app').innerHTML=(up.length?`<div class="sec">Upcoming Trips</div>`+up.map(row).join(''):'')+(past.length?`<div class="sec">Past Trips</div>`+past.map(row).join(''):'');
  hydrate();setFab(()=>tripForm())}
document.addEventListener('click',e=>{const t=e.target.closest('[data-trip]');if(t)location.hash='t-'+t.dataset.trip});

/* ================= trip view ================= */
function entries(trId){const out=[];for(const it of tripItems(trId)){if(it.type==='settle')continue;const s=splitDT(it.start),en=splitDT(it.end);
  const pair=it.type==='hotel'?['Check in','Check out']:it.type==='car'?['Pick up','Drop off']:null;
  out.push({it,d:s.d||'undated',t:s.t,lead:pair?pair[0]:'',first:true});if(pair&&en.d&&en.d!==s.d)out.push({it,d:en.d,t:en.t,lead:pair[1],first:false})}
  const sk=e=>e.d+(e.t||(!e.lead?'00':e.lead==='Check out'||e.lead==='Drop off'?'98':'99'));out.sort((a,b)=>sk(a).localeCompare(sk(b)));
  const res=[];out.forEach((e,i)=>{res.push(e);if(e.it.type==='flight'&&e.it.end){const nx=out[i+1];const a=mins(e.it.end),b=nx&&nx.it.type==='flight'&&mins(nx.it.start);if(nx&&a!=null&&b&&b>a&&b-a<1440)res.push({lay:true,d:e.d,mins:b-a,where:nx.it.location||''})}});return res}
function lines(it,e){const L=[],en=splitDT(it.end);
  if(it.type==='flight'){const fl=(it.details||[]).find(d=>/^flight$/i.test(d.label));L.push([fl?fl.value:'',it.provider?`(${it.provider})`:''].filter(Boolean).join(' '));if(en.t)L.push('Arrival: '+(en.d!==splitDT(it.start).d?dayLbl(en.d)+', ':'')+en.t)}
  else if(e.lead){L.push(e.lead+(e.t?': '+e.t:''));if(it.address)L.push(it.address);else if(it.location)L.push(it.location)}
  else{if(it.location)L.push(it.location);if(it.notes)L.push(String(it.notes).split(/(?<=[.!?])\s/)[0].slice(0,120))}
  return L.filter(Boolean)}
function renderTrip(id,tab){const tr=S.trips.get(id);
  $('#top').innerHTML=`<button class="ib" id="back" aria-label="All trips">${BACK}</button>${syncBtn()}<div class="sp"></div><button class="pill" id="editTrip">Edit trip</button>`;drawSync();
  $('#back').onclick=()=>location.hash='trips';$('#editTrip').onclick=()=>tripForm(tr);
  const L=ledger(tr),o=oweLine(tr,L);
  $('#head').innerHTML=`<div class="thead"><div><h1>${esc(tr.name)}</h1><p class="lede">${esc(dayLbl(tr.start))} - ${esc(dayLbl(tr.end,{weekday:'short',month:'short',day:'numeric',year:'numeric'}))}</p></div>${thumb(tr)}</div>
   <div class="chips"><span class="chip">Trip total <b>${cadStr(L.total)}</b></span>${o.solo?'<span class="chip">Solo trip</span>':`<span class="chip ${o.from?'owe':''}">${o.even?'<b>All square</b>':`${esc(o.from.name)} owes ${esc(o.to.name)} <b>${cadStr(o.amt)}</b>`}</span>`}</div>
   <div class="tabs" role="tablist"><button class="tab" role="tab" aria-selected="${tab==='plan'}" data-go="t-${esc(id)}">Itinerary</button><button class="tab" role="tab" aria-selected="${tab==='money'}" data-go="m-${esc(id)}">Money</button></div>`;
  document.querySelectorAll('[data-go]').forEach(b=>b.onclick=()=>location.hash=b.dataset.go);
  tab==='money'?renderMoney(tr,L):renderPlan(tr);hydrate();
  setFab(()=>menu([['Add an expense',solo(tr)?'Dinner, taxi, tickets':'Dinner, taxi, tickets: who paid and how to split',()=>expenseForm(tr)],['Add a booking or plan','Flight, hotel, car, activity, meal',()=>itemForm(tr,null)],solo(tr)?null:['Record a payment','Settle up between you',()=>{const o2=oweLine(tr,ledger(tr));o2.from?settleForm(tr,o2):toast('You’re all square')}]]))}
function renderPlan(tr){const ents=entries(tr.id);const days=new Map();ents.forEach(e=>{if(!days.has(e.d))days.set(e.d,[]);days.get(e.d).push(e)});
  if(tr.start&&tr.end&&tr.end>=tr.start&&(dObj(tr.end)-dObj(tr.start))/864e5<120){for(let d=tr.start;d<=tr.end;d=addDays(d,1))if(!days.has(d))days.set(d,[])}
  const keys=[...days.keys()].sort();if(!keys.length){$('#app').innerHTML=`<div class="empty">Nothing planned yet. Tap + to add a booking or an expense.</div>`;return}
  $('#app').innerHTML=keys.map(d=>{const list=days.get(d);
    return `<section class="day"><div class="dayh">${esc(dayLbl(d,{weekday:'long',month:'long',day:'numeric'}))}</div>${list.length?list.map((e,ix)=>{const fl=(ix===0?' f':'')+(ix===list.length-1?' l':'');
      if(e.lay)return `<div class="ent lay${fl}"><div class="tm"></div><div class="rail">${dot('clock')}</div><div class="bd"><span>${esc(dur(e.mins))} layover${e.where?' in '+esc(e.where):''}</span></div></div>`;
      const it=e.it,c=it.cost,cad=e.first&&hasCost(it)?cadOf(it,tr):null;
      const cl=e.first&&hasCost(it)?`<span class="c ${c.paidBy||solo(tr)?'':'need'}">${cad==null?esc(money(+c.amount,c.currency)):cadStr(cad)}${solo(tr)?'':' · '+(c.paidBy?esc(pname(tr,c.paidBy))+' paid':'who paid?')}</span>`:'';
      return `<button class="ent${fl}" data-item="${esc(it.id)}"><div class="tm">${e.t?esc(e.t):''}</div><div class="rail">${dot(it.type)}</div><div class="bd"><b>${esc(it.title||TYPES[it.type]||'Item')}${e.first&&it.todo?`<span class="flag">${esc(it.todo)}</span>`:''}</b>${lines(it,e).map(l=>`<span>${esc(l)}</span>`).join('')}${cl}</div></button>`}).join(''):`<div class="free">Free day</div>`}</section>`}).join('')}
document.addEventListener('click',e=>{const b=e.target.closest('[data-item]');if(b){const it=S.items.get(b.dataset.item);if(it)showItem(it)}});

function renderMoney(tr,L){const ps=people(tr),o=oweLine(tr,L),maxPaid=Math.max(1,...ps.map(p=>L.paid[p.id]||0));
  const costs=tripItems(tr.id).filter(hasCost).sort((a,b)=>String(a.start||'').localeCompare(String(b.start||'')));
  const cats=Object.entries(L.cats).sort((a,b)=>b[1]-a[1]);const used=[...new Set(costs.map(i=>i.cost.currency||'CAD').filter(c=>c!=='CAD').concat(Object.keys(tr.rates||{})))];
  $('#app').innerHTML=`<div class="card">${o.solo?`<p class="note" style="margin:0 0 4px">Trip total</p><div class="big">${cadStr(L.total)}</div>`:o.even?`<p class="note" style="margin:0 0 4px">Balance</p><div class="big">All square</div>`:`<p style="margin:0 0 4px;color:var(--soft)">${esc(o.from.name)} owes ${esc(o.to.name)}</p><div class="big coral">${cadStr(o.amt)}</div><div class="acts"><button class="btn" id="settle">Record a payment</button></div>`}</div>
    ${L.missing.length?`<div class="warn">${L.missing.length} cost${L.missing.length>1?'s are':' is'} in a currency with no rate yet, so ${L.missing.length>1?'they’re':'it’s'} left out of the totals. Add the rate below.</div>`:''}
    ${L.unassigned.length?`<div class="warn">${L.unassigned.length} cost${L.unassigned.length>1?'s don’t':' doesn’t'} say who paid. Tap ${L.unassigned.length>1?'them':'it'} below to set the payer.</div>`:''}
    ${o.solo?'':`<h2>Who paid what · ${cadStr(L.total)} total</h2><div>${ps.map((p,i)=>`<div class="brow"><span>${esc(p.name)}</span><div class="track ${i?'r':''}"><i style="width:${((L.paid[p.id]||0)/maxPaid*100).toFixed(1)}%"></i></div><span class="v">${cadStr(L.paid[p.id]||0)}</span></div>`).join('')}</div><p class="note">Fair share: ${ps.map(p=>`${esc(p.name)} ${cadStr(L.share[p.id]||0)}`).join(' · ')}</p>`}
    ${cats.length?`<h2>By category</h2><div>${cats.map(([k,v])=>`<div class="brow"><span>${esc(CATS[k]||k)}</span><div class="track"><i style="width:${(v/cats[0][1]*100).toFixed(1)}%"></i></div><span class="v">${cadStr(v)}</span></div>`).join('')}</div>`:''}
    <h2>${o.solo?'All costs':'All costs and payments'}</h2>${costs.length?costs.map(it=>{const cad=cadOf(it,tr),c=it.cost,isS=it.type==='settle';const to=isS?Object.keys(c.custom||{})[0]:'';
      return `<button class="costrow" data-item="${esc(it.id)}"><div><b>${esc(isS?`${pname(tr,c.paidBy)} paid ${pname(tr,to)}`:it.title)}</b><span>${esc(dayLbl(splitDT(it.start).d))}${isS?' · Payment':o.solo?'':' · '+esc(c.paidBy?pname(tr,c.paidBy)+' paid':'Payer not set')+' · '+esc((SPLITS.find(s=>s[0]===(c.split||'half'))||SPLITS[0])[1])}</span></div><div class="v">${cad==null?'—':cadStr(cad)}${(c.currency||'CAD')!=='CAD'?`<span style="display:block;font-weight:400">${esc(money(+c.amount,c.currency))}</span>`:''}</div></button>`}).join(''):`<p class="note">No costs yet.</p>`}
    <h2>Exchange rates</h2><p class="note">Canadian dollars per unit. Copy today’s rate from Wise, or enter the exact CAD charged on any cost.${tr.ratesUpdated?' Last updated '+esc(tr.ratesUpdated)+'.':''}</p>
    <div>${used.map(c=>`<div class="rate"><b>1 ${esc(c)}</b><input inputmode="decimal" data-rate="${esc(c)}" value="${esc(tr.rates&&tr.rates[c]!=null?tr.rates[c]:'')}" placeholder="CAD"><span class="note">CAD</span></div>`).join('')||'<p class="note">All costs are in CAD so far.</p>'}</div>
    <div class="acts"><select id="addCur" class="sel" style="width:auto" aria-label="Add a currency"><option value="">Add currency…</option>${CURS.filter(c=>c!=='CAD'&&!used.includes(c)).map(c=>`<option>${c}</option>`).join('')}</select><button class="btn ghost" id="saveRates">Save rates</button></div>`;
  const st=$('#settle');if(st)st.onclick=()=>settleForm(tr,o);
  $('#addCur').onchange=e=>{const c=e.target.value;if(!c)return;tr.rates=Object.assign({},tr.rates,{[c]:(tr.rates||{})[c]??''});renderMoney(tr,L)};
  $('#saveRates').onclick=()=>{const r={};document.querySelectorAll('[data-rate]').forEach(i=>{const v=parseFloat(i.value);if(v>0)r[i.dataset.rate]=v});putTrip(Object.assign({},S.trips.get(tr.id),{rates:r,ratesUpdated:dayLbl(today(),{month:'short',day:'numeric',year:'numeric'})}));toast('Rates saved')}}

/* ================= sheets ================= */
function openSheet(html){const s=$('#sheet');s.innerHTML=html;$('#scrim').classList.add('on');s.querySelectorAll('.x,[data-close]').forEach(b=>b.onclick=closeSheet);s.scrollTop=0}
function closeSheet(){$('#scrim').classList.remove('on');$('#sheet').innerHTML=''}
$('#scrim').addEventListener('click',e=>{if(e.target.id==='scrim')closeSheet()});
document.addEventListener('keydown',e=>{if(e.key==='Escape'&&$('#scrim').classList.contains('on'))closeSheet()});
const hdr=(title,sub,ic)=>`<div class="shd">${ic?dot(ic):''}<div style="flex:1"><h3>${esc(title)}</h3>${sub?`<p>${esc(sub)}</p>`:''}</div><button class="x" aria-label="Close">×</button></div>`;
function menu(opts){opts=opts.filter(Boolean);openSheet(hdr('Add','')+`<div class="sb menu">${opts.map((o,i)=>`<button data-m="${i}"><div><div>${esc(o[0])}</div><span>${esc(o[1])}</span></div></button>`).join('')}</div>`);$('#sheet').querySelectorAll('[data-m]').forEach(b=>b.onclick=()=>opts[+b.dataset.m][2]())}
function confirmBox(text,yes,fn){openSheet(hdr(text,'')+`<div class="sb"><div class="acts"><button class="btn red" id="cy">${esc(yes)}</button><button class="btn ghost" data-close>Cancel</button></div></div>`);$('#cy').onclick=async()=>{await fn();closeSheet()}}
const seg=(name,opts,val)=>`<div class="seg" data-seg="${name}">${opts.map(([v,l])=>`<button type="button" data-v="${esc(v)}" aria-pressed="${v===val}">${esc(l)}</button>`).join('')}</div>`;
function wireSeg(root,cb){root.querySelectorAll('[data-seg]').forEach(g=>g.querySelectorAll('button').forEach(b=>b.onclick=()=>{g.querySelectorAll('button').forEach(x=>x.setAttribute('aria-pressed',x===b));cb&&cb(g.dataset.seg,b.dataset.v)}))}
const segVal=(root,n)=>{const b=root.querySelector(`[data-seg="${n}"] [aria-pressed=true]`);return b?b.dataset.v:''};
const curSel=(id,v)=>`<select id="${id}">${CURS.map(c=>`<option ${c===v?'selected':''}>${c}</option>`).join('')}</select>`;
function addPeek(root){(root||document).querySelectorAll('input[type=password]:not([data-peek])').forEach(inp=>{inp.dataset.peek='1';const w=document.createElement('div');w.style.cssText='display:flex;gap:8px;align-items:center;margin-top:4px';
  inp.parentNode.insertBefore(w,inp);w.appendChild(inp);inp.style.marginTop='0';inp.style.flex='1';const b=document.createElement('button');b.type='button';b.className='btn ghost';b.style.cssText='min-height:44px;padding:8px 14px;flex:none';b.textContent='Show';
  b.onclick=()=>{const show=inp.type==='password';inp.type=show?'text':'password';b.textContent=show?'Hide':'Show';inp.focus()};w.appendChild(b)})}
let fabFn=null;function setFab(fn){fabFn=fn;$('#fab').hidden=!fn}
$('#fab').onclick=()=>fabFn&&fabFn();

function showItem(it){const tr=S.trips.get(it.tripId);if(!tr)return;const s=splitDT(it.start),en=splitDT(it.end),c=it.cost||{};const cad=cadOf(it,tr);const ps=people(tr);
  const when=(d,t)=>d?esc(dayLbl(d,{weekday:'short',month:'short',day:'numeric',year:'numeric'}))+(t?', '+esc(t):''):'—';
  const lbl=it.type==='hotel'?['Check in','Check out']:it.type==='car'?['Pick up','Drop off']:it.type==='flight'?['Departs','Arrives']:['Starts','Ends'];
  const maps=it.address||it.location?'https://www.google.com/maps/search/?api=1&query='+encodeURIComponent(it.address||it.location):'';
  const src=httpsOnly(it.source&&it.source.link),isS=it.type==='settle',has=hasCost(it);
  openSheet(hdr(isS?'Payment':it.title||TYPES[it.type],[TYPES[it.type]||'',it.provider].filter(Boolean).join(' · '),it.type)+`<div class="sb">
    ${it.todo?`<p><span class="flag" style="margin:0">${esc(it.todo)}</span></p>`:''}
    <dl class="kv"><dt>${lbl[0]}</dt><dd>${when(s.d,s.t)}</dd>${en.d?`<dt>${lbl[1]}</dt><dd>${when(en.d,en.t)}</dd>`:''}
    ${it.location?`<dt>Where</dt><dd>${esc(it.location)}</dd>`:''}${it.address?`<dt>Address</dt><dd>${esc(it.address)}</dd>`:''}${maps?`<dt></dt><dd><a href="${esc(maps)}" target="_blank" rel="noopener">Open in Google Maps</a></dd>`:''}
    ${it.phone?`<dt>Phone</dt><dd><a href="tel:${esc(String(it.phone).replace(/[^+\d]/g,''))}">${esc(it.phone)}</a></dd>`:''}
    ${it.confirmation?`<dt>Confirmation</dt><dd><b style="font-size:20px;letter-spacing:.04em">${esc(it.confirmation)}</b><button class="copy" data-copy="${esc(it.confirmation)}">Copy</button></dd>`:''}
    ${(it.details||[]).map(d=>`<dt>${esc(d.label)}</dt><dd>${/^https:\/\//.test(d.value)?`<a href="${esc(d.value)}" target="_blank" rel="noopener">${esc(d.value)}</a>`:esc(d.value)}${/(number|code|ref|#|pnr|booking)/i.test(d.label)&&String(d.value).length<30?`<button class="copy" data-copy="${esc(d.value)}">Copy</button>`:''}</dd>`).join('')}
    ${it.notes?`<dt>Notes</dt><dd style="white-space:pre-wrap">${esc(it.notes)}</dd>`:''}</dl>
    ${!isS?`<div class="card" style="background:var(--bg)"><div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap"><b>Cost</b><span class="big" style="font-size:28px">${has?esc(money(+c.amount,c.currency||'CAD')):'Not set'}</span></div>
      ${has&&(c.currency||'CAD')!=='CAD'?`<p class="note" style="margin:2px 0 0">${cad==null?'Add a '+esc(c.currency)+' rate on the Money tab to convert.':'≈ '+cadStr(cad)+(c.cad!=null&&c.cad!==''?' (actual CAD charged)':' at the trip rate')}</p>`:''}
      ${has&&!solo(tr)?`<label class="f" style="margin-top:14px">Paid by</label>${seg('paidBy',ps.map(p=>[p.id,p.name]),c.paidBy||'')}<label class="f">Split</label>${seg('split',SPLITS,c.split||'half')}<div id="custom"></div><p class="note" id="shareNote"></p>`:has?'':`<p class="note">Edit to add what it cost.</p>`}</div>`:''}
    <h2 style="font-size:17px">Attachments</h2><div>${(it.attachments||[]).map(a=>`<div class="att"><a data-att="${esc(a.id)}" target="_blank" rel="noopener" download="${esc(a.name)}">${esc(a.name)}</a><button class="copy" data-rm="${esc(a.id)}">Remove</button></div>`).join('')||'<p class="note">Boarding passes, tickets and PDFs live here, and work offline.</p>'}</div>
    <label class="btn ghost" style="margin-top:10px">Attach a file<input type="file" id="file" accept="application/pdf,image/*" class="vh"></label>
    ${src?`<p style="margin-top:16px"><a href="${esc(src)}" target="_blank" rel="noopener">${/claude\.ai/.test(src)?'Open the itinerary doc':'Open the original email'}</a></p>`:''}
    <div class="acts">${!isS?`<button class="btn" id="edit">Edit</button>`:''}<button class="btn ghost" id="del">Delete</button></div></div>`);
  const sh=$('#sheet');hydrate(sh);sh.querySelectorAll('[data-copy]').forEach(b=>b.onclick=async()=>{try{await navigator.clipboard.writeText(b.dataset.copy);toast('Copied')}catch(e){toast('Select the text to copy it')}});
  const drawCustom=()=>{const box=$('#custom');if(!box)return;const sp=segVal(sh,'split');if(sp==='custom'){box.innerHTML=`<div class="two">${ps.map(p=>`<label class="f">${esc(p.name)}’s part (${esc(c.currency||'CAD')})<input inputmode="decimal" data-cust="${esc(p.id)}" value="${esc(c.custom&&c.custom[p.id]!=null?c.custom[p.id]:'')}"></label>`).join('')}</div><button class="btn ghost" id="saveCust" style="margin-top:10px">Save split</button>`;
      $('#saveCust').onclick=()=>{const cu={};sh.querySelectorAll('[data-cust]').forEach(i=>cu[i.dataset.cust]=parseFloat(i.value)||0);it.cost=Object.assign({},c,{split:'custom',custom:cu});putItem(it);toast('Split saved')}}else box.innerHTML='';
    const n=$('#shareNote');if(n&&cad!=null){const shs=sharesOf(it,tr,cad);n.textContent=c.paidBy?ps.map(p=>`${p.name}’s share ${cadStr(shs[p.id])}`).join(' · '):'Choose who paid to count it in the balance.'}};
  wireSeg(sh,(k,v)=>{const nc=Object.assign({},it.cost||{});nc[k]=v;it.cost=nc;Object.assign(c,nc);drawCustom();if(!(k==='split'&&v==='custom'))putItem(it)});drawCustom();
  const ed=$('#edit');if(ed)ed.onclick=()=>itemForm(tr,it);
  $('#del').onclick=()=>confirmBox(`Delete “${isS?'this payment':it.title}”?`,'Delete',()=>{delItem(it.id);toast('Deleted')});
  sh.querySelectorAll('[data-rm]').forEach(b=>b.onclick=()=>{it.attachments=(it.attachments||[]).filter(a=>a.id!==b.dataset.rm);putItem(it);showItem(S.items.get(it.id)||it)});
  $('#file').onchange=async e=>{const file=e.target.files[0];if(!file)return;if(file.size>25*1024*1024)return toast('Files must be under 25 MB',5000);
    try{const id=await addBlob(file);it.attachments=(it.attachments||[]).concat([{id,name:file.name,type:file.type,size:file.size}]);putItem(it);toast(navigator.onLine?'Attached. Uploading…':'Attached. It uploads when you’re back online.');showItem(S.items.get(it.id)||it)}catch(x){toast('Couldn’t attach: '+x.message)}}}

function itemForm(tr,it){const isNew=!it;it=it?clean(it):{id:uid('i'),tripId:tr.id,type:'activity',cost:{currency:'CAD',paidBy:me(),split:'half'}};const s=splitDT(it.start),en=splitDT(it.end),c=it.cost||{};
  openSheet(hdr(isNew?'Add a booking or plan':'Edit','')+`<form class="sb" id="frm">
   <label class="f">Type<select id="fType">${Object.entries(TYPES).filter(([k])=>k!=='settle').map(([k,v])=>`<option value="${k}" ${k===it.type?'selected':''}>${v}</option>`).join('')}</select></label>
   <label class="f">Name<input id="fTitle" required value="${esc(it.title)}" placeholder="Hotel name, or YOW → YYZ"></label>
   <div class="two"><label class="f">Start date<input type="date" id="fSd" required value="${esc(s.d||tr.start||'')}"></label><label class="f">Start time<input type="time" id="fSt" value="${esc(s.t)}"></label>
   <label class="f">End date<input type="date" id="fEd" value="${esc(en.d)}"></label><label class="f">End time<input type="time" id="fEt" value="${esc(en.t)}"></label></div>
   <div class="two"><label class="f">Confirmation #<input id="fConf" value="${esc(it.confirmation)}"></label><label class="f">Company<input id="fProv" value="${esc(it.provider)}"></label></div>
   <label class="f">City / place<input id="fLoc" value="${esc(it.location)}"></label><label class="f">Address<input id="fAddr" value="${esc(it.address)}"></label><label class="f">Phone<input id="fPhone" value="${esc(it.phone)}"></label>
   <div class="three"><label class="f">Cost<input id="fAmt" inputmode="decimal" value="${esc(c.amount??'')}"></label><label class="f">Currency${curSel('fCur',c.currency||'CAD')}</label></div>
   <label class="f">Actual CAD charged (optional)<input id="fCad" inputmode="decimal" value="${esc(c.cad??'')}" placeholder="From your card or Wise statement"></label>
   <label class="f">Reminder tag (optional)<input id="fTodo" value="${esc(it.todo)}" placeholder="Book ahead"></label>
   <label class="f">Other details, one per line as Label: value<textarea id="fDet" rows="4">${esc((it.details||[]).map(d=>d.label+': '+d.value).join('\n'))}</textarea></label>
   <label class="f">Notes<textarea id="fNotes" rows="3">${esc(it.notes)}</textarea></label>
   <div class="acts"><button class="btn" type="submit">Save</button><button class="btn ghost" type="button" data-close>Cancel</button></div></form>`);
  $('#frm').onsubmit=e=>{e.preventDefault();const v=id=>$(id).value.trim();
    it.type=v('#fType');it.title=v('#fTitle');it.start=v('#fSd')+(v('#fSt')?'T'+v('#fSt'):'');it.end=v('#fEd')?v('#fEd')+(v('#fEt')?'T'+v('#fEt'):''):'';
    Object.assign(it,{confirmation:v('#fConf'),provider:v('#fProv'),location:v('#fLoc'),address:v('#fAddr'),phone:v('#fPhone'),notes:v('#fNotes'),todo:v('#fTodo')});
    it.details=v('#fDet').split('\n').map(l=>{const i=l.indexOf(':');return i>0?{label:l.slice(0,i).trim(),value:l.slice(i+1).trim()}:null}).filter(Boolean);
    const amt=v('#fAmt'),cadv=v('#fCad');it.cost=Object.assign({},it.cost||{},{amount:amt===''?null:parseFloat(amt),currency:v('#fCur'),cad:cadv===''?null:parseFloat(cadv)});
    if(!it.cost.paidBy&&it.cost.amount!=null)it.cost.paidBy=me();if(!it.cost.split)it.cost.split='half';
    it.category=typeCat[it.type]||'other';putItem(it);closeSheet();toast('Saved')}}

function expenseForm(tr){const ps=people(tr),last=[...tripItems(tr.id)].reverse().find(i=>i.type==='expense'||i.type==='restaurant');const d0=today(),d=tr.start&&d0<tr.start?tr.start:tr.end&&d0>tr.end?tr.end:d0;
  const cur0=(last&&last.cost&&last.cost.currency)||Object.keys(tr.rates||{})[0]||'CAD';const payer0=ps.some(p=>p.id===me())?me():ps[0].id;
  openSheet(hdr('Add an expense','','expense')+`<form class="sb" id="frm">
   <label class="f">What was it?<input id="eT" required placeholder="Dinner at Tasca" autocomplete="off"></label>
   <div class="three"><label class="f">Amount<input id="eA" required inputmode="decimal" placeholder="0.00"></label><label class="f">Currency${curSel('eC',cur0)}</label></div>
   ${ps.length<2?'':`<label class="f">Paid by</label>${seg('paidBy',ps.map(p=>[p.id,p.name]),payer0)}<label class="f">Split</label>${seg('split',SPLITS.filter(s=>s[0]!=='custom'),'half')}`}
   <label class="f">Category</label>${seg('cat',Object.entries(CATS),'food')}
   <div class="two"><label class="f">Date<input type="date" id="eD" value="${esc(d)}"></label><label class="f">Time (optional)<input type="time" id="eTm"></label></div>
   <div class="acts"><button class="btn" type="submit">Add expense</button><button class="btn ghost" type="button" data-close>Cancel</button></div></form>`);
  const sh=$('#sheet');wireSeg(sh);setTimeout(()=>{try{$('#eT').focus()}catch(e){}},50);
  $('#frm').onsubmit=e=>{e.preventDefault();const amt=parseFloat($('#eA').value.replace(',','.'));if(!(amt>0))return toast('Enter an amount');
    const cat=segVal(sh,'cat'),cur=$('#eC').value;putItem({id:uid('i'),tripId:tr.id,type:cat==='food'?'restaurant':'expense',category:cat,title:$('#eT').value.trim(),start:$('#eD').value+($('#eTm').value?'T'+$('#eTm').value:''),
      cost:{amount:amt,currency:cur,paidBy:segVal(sh,'paidBy')||payer0,split:segVal(sh,'split')||'half'},createdAt:Date.now()});closeSheet();toast(`Added ${money(amt,cur)}`)}}

function settleForm(tr,o){openSheet(hdr('Record a payment','','settle')+`<form class="sb" id="frm"><p>${esc(o.from.name)} pays ${esc(o.to.name)} back, by e-Transfer, cash or anything else.</p>
   <label class="f">Amount (CAD)<input id="sA" inputmode="decimal" value="${o.amt.toFixed(2)}"></label><label class="f">Date<input type="date" id="sD" value="${today()}"></label>
   <div class="acts"><button class="btn" type="submit">Record payment</button><button class="btn ghost" type="button" data-close>Cancel</button></div></form>`);
  $('#frm').onsubmit=e=>{e.preventDefault();const a=parseFloat($('#sA').value);if(!(a>0))return toast('Enter an amount');
    putItem({id:uid('i'),tripId:tr.id,type:'settle',title:'Payment',start:$('#sD').value,cost:{amount:a,currency:'CAD',paidBy:o.from.id,split:'custom',custom:{[o.to.id]:a}},createdAt:Date.now()});closeSheet();toast('Payment recorded')}}

function tripForm(tr){const isNew=!tr;tr=tr?clean(tr):{id:uid('t'),people:clean(DEFAULT_PEOPLE),rates:{}};const ps=people(tr);
  openSheet(hdr(isNew?'New trip':'Edit trip','')+`<form class="sb" id="frm">
   <div class="cover">${thumb(tr)}<div style="display:flex;flex-wrap:wrap;gap:8px"><button type="button" class="btn ghost" id="cFind" style="min-height:40px">${tr.cover?'Find another photo':'Find a photo'}</button><label class="btn ghost" style="min-height:40px">Use my photo<input type="file" id="tPh" accept="image/*" class="vh"></label>${tr.photo||tr.cover?`<button type="button" class="btn ghost" id="cDraw" style="min-height:40px">Use drawing</button>`:''}</div></div>
   <label class="f">Trip name<input id="tN" required value="${esc(tr.name)}" placeholder="Azores"></label>
   <div class="two"><label class="f">Starts<input type="date" id="tS" required value="${esc(tr.start)}"></label><label class="f">Ends<input type="date" id="tE" required value="${esc(tr.end)}"></label></div>
   <div class="two"><label class="f">Traveller 1<input id="tP0" value="${esc(ps[0].name)}"></label><label class="f">Traveller 2<input id="tP1" value="${esc(ps[1]?ps[1].name:'')}" placeholder="Leave empty for a solo trip"></label></div>
   <div class="acts"><button class="btn" type="submit">Save trip</button>${!isNew?`<button class="btn ghost" type="button" id="tDel">Delete trip</button>`:''}</div></form>`);
  const redraw=()=>{const th=$('#sheet .cover .th');if(th){th.outerHTML=thumb(tr);hydrate($('#sheet'))}};
  $('#cFind').onclick=async()=>{const name=$('#tN').value.trim()||tr.name;if(!name)return toast('Type the trip name first');if(!navigator.onLine)return toast('Finding a photo needs internet');
    toast('Looking for a photo…',8000);try{const c=await findCover(name,tr.cover?(tr.cover.idx||0)+1:0);if(!c)return toast('No photo found. Try a different trip name, or use your own.');delete tr.photo;tr.cover=c;redraw();toast('Found one. Save the trip to keep it.')}catch(e){toast('Couldn’t reach Wikimedia: '+e.message)}};
  $('#tPh').onchange=async e=>{const f=e.target.files[0];if(!f)return;if(f.size>25*1024*1024)return toast('That photo is over 25 MB');try{tr.photo=await addBlob(f);tr.photoType=f.type;delete tr.cover;redraw();toast('Photo ready. Save the trip to keep it.')}catch(x){toast('Couldn’t use that photo: '+x.message)}};
  const cd=$('#cDraw');if(cd)cd.onclick=()=>{delete tr.photo;delete tr.cover;redraw();cd.remove()};
  $('#frm').onsubmit=async e=>{e.preventDefault();tr.name=$('#tN').value.trim();tr.start=$('#tS').value;tr.end=$('#tE').value;if(tr.end<tr.start)return toast('The trip ends before it starts');
    const p2=$('#tP1').value.trim();tr.people=[{id:'a',name:$('#tP0').value.trim()||'Me'}].concat(p2&&!/^travell?er 2$/i.test(p2)?[{id:'b',name:p2}]:[]);tr.currency='CAD';
    if(isNew&&!tr.photo&&!tr.cover&&navigator.onLine){try{const c=await findCover(tr.name);if(c)tr.cover=c}catch(x){}}
    putTrip(tr);closeSheet();location.hash='t-'+tr.id};
  const d=$('#tDel');if(d)d.onclick=()=>confirmBox(`Delete ${tr.name} and everything in it?`,'Delete trip',()=>{delTrip(tr.id);location.hash='trips'})}

/* ================= vault (encrypted on the phone before it syncs) ================= */
const V={key:null,data:null,timer:null};
const PRF_SALT=te.encode('trip-ledger-vault-prf-v1').slice(0,32);
const rand=n=>crypto.getRandomValues(new Uint8Array(n));
async function passKey(pass,salt,iter){const base=await crypto.subtle.importKey('raw',te.encode(pass.normalize('NFKC')),'PBKDF2',false,['deriveKey']);return crypto.subtle.deriveKey({name:'PBKDF2',salt,iterations:iter,hash:'SHA-256'},base,{name:'AES-GCM',length:256},false,['encrypt','decrypt'])}
async function prfKey(secret){const base=await crypto.subtle.importKey('raw',secret,'HKDF',false,['deriveKey']);return crypto.subtle.deriveKey({name:'HKDF',hash:'SHA-256',salt:new Uint8Array(32),info:te.encode('trip-ledger vault wrap')},base,{name:'AES-GCM',length:256},false,['encrypt','decrypt'])}
async function seal(key,bytes){const iv=rand(12);const ct=new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM',iv},key,bytes));return{iv:b64enc(iv),ct:b64enc(ct)}}
async function open_(key,box){return new Uint8Array(await crypto.subtle.decrypt({name:'AES-GCM',iv:b64dec(box.iv)},key,b64dec(box.ct)))}
const masterFrom=raw=>crypto.subtle.importKey('raw',raw,'AES-GCM',false,['encrypt','decrypt']);
async function vaultCreate(pass){const raw=rand(32),salt=rand(16),iter=600000;const wk=await passKey(pass,salt,iter);const master=await masterFrom(raw);
  const data={entries:[]};const v={v:1,kdf:{salt:b64enc(salt),iter},wraps:[Object.assign({t:'pass'},await seal(wk,raw))],data:await seal(master,te.encode(JSON.stringify(data))),updatedAt:Date.now()};
  change('vault.json',v);V.key=master;V.raw=raw;V.data=data;armLock()}
async function vaultUnlockPass(pass){const v=S.vault;const wk=await passKey(pass,b64dec(v.kdf.salt),v.kdf.iter);const w=v.wraps.find(x=>x.t==='pass');const raw=await open_(wk,w);await finishUnlock(raw)}
async function finishUnlock(raw){V.raw=raw;V.key=await masterFrom(raw);V.data=JSON.parse(td.decode(await open_(V.key,S.vault.data)));armLock()}
async function vaultSave(){const v=clean(S.vault);v.data=await seal(V.key,te.encode(JSON.stringify(V.data)));v.updatedAt=Date.now();change('vault.json',v)}
function vaultLock(){V.key=null;V.raw=null;V.data=null;clearTimeout(V.timer);for(const k of Object.keys(S.urls))if(k.startsWith('v:')){URL.revokeObjectURL(S.urls[k]);delete S.urls[k]}if(location.hash==='#vault')route()}
function armLock(){clearTimeout(V.timer);V.timer=setTimeout(vaultLock,3*60*1000)}
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='hidden')vaultLock()});
['click','keydown','touchstart'].forEach(ev=>document.addEventListener(ev,()=>{if(V.key)armLock()},{passive:true}));
const prfSupported=()=>!!(window.PublicKeyCredential&&navigator.credentials&&navigator.credentials.create);
async function prfGet(credId){const a=await navigator.credentials.get({publicKey:{challenge:rand(32),allowCredentials:credId?[{type:'public-key',id:b64dec(credId)}]:[],userVerification:'required',timeout:60000,extensions:{prf:{eval:{first:PRF_SALT}}}}});
  const r=a.getClientExtensionResults();const out=r&&r.prf&&r.prf.results&&r.prf.results.first;if(!out)throw new Error('This phone can’t use fingerprint unlock for the vault. Use the passphrase.');return{secret:new Uint8Array(out),id:b64enc(new Uint8Array(a.rawId))}}
async function fingerprintEnroll(){const nm=pname(null,me());const cred=await navigator.credentials.create({publicKey:{challenge:rand(32),rp:{name:'Jetsetr',id:location.hostname},user:{id:rand(16),name:nm+' · Jetsetr vault',displayName:nm},
    pubKeyCredParams:[{type:'public-key',alg:-7},{type:'public-key',alg:-257}],authenticatorSelection:{residentKey:'preferred',userVerification:'required'},timeout:60000,extensions:{prf:{eval:{first:PRF_SALT}}}}});
  const ext=cred.getClientExtensionResults();if(!ext||!ext.prf||ext.prf.enabled===false)throw new Error('This phone can’t use fingerprint unlock for the vault yet. The passphrase still works.');
  const id=b64enc(new Uint8Array(cred.rawId));let secret=ext.prf.results&&ext.prf.results.first?new Uint8Array(ext.prf.results.first):null;if(!secret)secret=(await prfGet(id)).secret;
  const wk=await prfKey(secret);const v=clean(S.vault);v.wraps=v.wraps.filter(w=>!(w.t==='pk'&&w.cred===id));v.wraps.push(Object.assign({t:'pk',cred:id,who:me(),name:nm+'’s phone',at:Date.now()},await seal(wk,V.raw)));v.updatedAt=Date.now();change('vault.json',v);LS.set('tl_cred',id)}
async function fingerprintUnlock(){const id=LS.get('tl_cred');const w=S.vault.wraps.find(x=>x.t==='pk'&&x.cred===id);if(!w)throw new Error('Fingerprint isn’t set up for the vault on this phone. Use the passphrase.');const{secret}=await prfGet(id);const raw=await open_(await prfKey(secret),w);await finishUnlock(raw)}
async function vaultImg(id){if(S.urls['v:'+id])return S.urls['v:'+id];const rec=await IDB.get('blobs',id);if(!rec)return null;const box=JSON.parse(td.decode(new Uint8Array(rec.buf)));const bytes=await open_(V.key,box);const u=URL.createObjectURL(new Blob([bytes],{type:box.type||'image/jpeg'}));S.urls['v:'+id]=u;return u}
function renderVault(){setTimeout(()=>addPeek($('#app')),0);$('#top').innerHTML=`<button class="ib" id="back" aria-label="All trips">${BACK}</button>${syncBtn()}<div class="sp"></div>${V.key?'<button class="pill" id="vLock">Lock</button>':''}`;drawSync();$('#back').onclick=()=>location.hash='trips';
  const lk=$('#vLock');if(lk)lk.onclick=vaultLock;setFab(null);
  $('#head').innerHTML=`<h1>Vault</h1><p class="lede">Passports and other sensitive details, scrambled on this phone before they sync. GitHub and Claude only ever see gibberish.</p>`;
  const A=$('#app');
  if(!S.vault){A.innerHTML=`<div class="card"><div class="lockbig">${svgI('lock',1.6)}</div><p>Choose a passphrase you’ll share with Tami. Four or five random words is strong and easy to type, like <b>kayak-lantern-orbit-maple-fog</b>.</p>
      <p class="note">If you both forget it, nobody can recover what’s in the vault, including Claude. Save it in your password manager.</p>
      <label class="f">Passphrase<input id="vp1" type="password" autocomplete="new-password"></label><label class="f">Type it again<input id="vp2" type="password" autocomplete="new-password"></label>
      <div class="acts"><button class="btn" id="vCreate">Create vault</button></div></div>`;
    $('#vCreate').onclick=async()=>{const a=$('#vp1').value,b=$('#vp2').value;if(a!==b)return toast('The two passphrases don’t match');if(a.length<12)return toast('Use at least 12 characters, ideally four or five words');
      toast('Creating your vault…',10000);try{await vaultCreate(a);toast('Vault created');route()}catch(e){toast('Couldn’t create the vault: '+e.message)}};return}
  if(!V.key){const hasPk=S.vault.wraps.some(w=>w.t==='pk'&&w.cred===LS.get('tl_cred'));
    A.innerHTML=`<div class="card" style="text-align:center"><div class="lockbig">${svgI('lock',1.6)}</div>${hasPk?`<button class="btn" id="vFp" style="width:100%">${svgI('finger',2).replace('<svg','<svg width="22" height="22"')} Unlock with fingerprint</button><p class="note">or use the passphrase</p>`:''}
      <label class="f" style="text-align:left">Passphrase<input id="vp" type="password" autocomplete="current-password"></label><div class="acts" style="justify-content:center"><button class="btn ${hasPk?'ghost':''}" id="vOpen">Unlock</button></div></div>`;
    const fp=$('#vFp');if(fp){fp.onclick=async()=>{try{await fingerprintUnlock();route()}catch(e){toast(e.name==='NotAllowedError'?'Fingerprint cancelled':e.message,6000)}};}
    const go=async()=>{toast('Unlocking…',8000);try{await vaultUnlockPass($('#vp').value);toast('Unlocked');route()}catch(e){toast('Wrong passphrase')}};$('#vOpen').onclick=go;$('#vp').onkeydown=e=>{if(e.key==='Enter')go()};return}
  const ents=V.data.entries||[];const groups={};ents.forEach(e=>{(groups[e.who||'Both']=groups[e.who||'Both']||[]).push(e)});
  const hasPk=S.vault.wraps.some(w=>w.t==='pk'&&w.cred===LS.get('tl_cred'));
  A.innerHTML=`${Object.keys(groups).length?Object.entries(groups).map(([g,list])=>`<h2>${esc(g)}</h2>${list.map(e=>`<div class="vault-e"><div><div class="l">${esc(e.label)}</div><div class="v">${esc(e.value)}</div>${e.img?`<img class="vimg" data-vimg="${esc(e.img)}" alt="">`:''}</div><div>${e.value?`<button class="copy" data-copy="${esc(e.value)}">Copy</button>`:''}<button class="copy" data-vdel="${esc(e.id)}">Delete</button></div></div>`).join('')}`).join(''):'<p class="empty">Empty for now. Add passport numbers, expiry dates, insurance policy numbers and so on.</p>'}
    <div class="card"><b>Add to the vault</b><label class="f">Whose is it?</label>${seg('who',[['Christian','Christian'],['Tami','Tami'],['Both','Both']],pname(null,me()))}
      <label class="f">What<input id="vl" placeholder="Passport number"></label><label class="f">Value<input id="vv" autocomplete="off" placeholder="AB123456"></label>
      <label class="btn ghost" style="margin-top:10px">Add a photo (optional)<input type="file" id="vf" accept="image/*" class="vh"></label><span class="note" id="vfn"></span>
      <div class="acts"><button class="btn" id="vAdd">Add</button></div></div>
    ${prfSupported()&&!hasPk?`<div class="card"><b>Fingerprint unlock</b><p class="note">Unlock the vault on this phone with your fingerprint instead of typing the passphrase.</p><div class="acts"><button class="btn ghost" id="vEnroll">Set up fingerprint</button></div></div>`:''}
    <p class="note">The vault locks itself after 3 minutes, or when you leave the app.</p>`;
  const sh=A;wireSeg(sh);let pic=null;$('#vf').onchange=e=>{pic=e.target.files[0]||null;$('#vfn').textContent=pic?pic.name:''};
  sh.querySelectorAll('[data-copy]').forEach(b=>b.onclick=async()=>{try{await navigator.clipboard.writeText(b.dataset.copy);toast('Copied')}catch(e){toast('Select the text to copy it')}});
  sh.querySelectorAll('[data-vdel]').forEach(b=>b.onclick=()=>confirmBox('Delete this vault entry?','Delete',async()=>{V.data.entries=V.data.entries.filter(x=>x.id!==b.dataset.vdel);await vaultSave();route()}));
  sh.querySelectorAll('img[data-vimg]').forEach(async img=>{const u=await vaultImg(img.dataset.vimg);if(u)img.src=u;else img.replaceWith(Object.assign(document.createElement('p'),{className:'note',textContent:'Photo downloads next time you’re online.'}))});
  $('#vAdd').onclick=async()=>{const l=$('#vl').value.trim(),v=$('#vv').value.trim();if(!l||(!v&&!pic))return toast('Add a label and a value or a photo');
    const e={id:uid('v'),who:segVal(sh,'who')||'Both',label:l,value:v};
    if(pic){if(pic.size>15*1024*1024)return toast('Photo must be under 15 MB');const box=await seal(V.key,new Uint8Array(await pic.arrayBuffer()));box.type=pic.type;const id=uid('f');
      await IDB.set('blobs',id,{buf:te.encode(JSON.stringify(box)).buffer,type:'application/json',name:'vault'});S.upQueue[id]=true;e.img=id;V.data.vaultFiles=(V.data.vaultFiles||[]).concat(id)}
    V.data.entries.push(e);await vaultSave();toast('Saved to the vault');route()};
  const en=$('#vEnroll');if(en)en.onclick=async()=>{try{await fingerprintEnroll();toast('Fingerprint unlock is on for this phone');route()}catch(e){toast(e.name==='NotAllowedError'?'Cancelled':e.message,7000)}}}

/* ================= settings & setup ================= */
function settingsSheet(){const c=S.cfg,n=Object.keys(S.pending).length+Object.keys(S.upQueue).length;
  openSheet(hdr('Sync','')+`<div class="sb"><dl class="kv"><dt>Status</dt><dd>${esc(S.sync.state==='err'?S.sync.msg:navigator.onLine?'Online':'Offline')}</dd><dt>Last synced</dt><dd>${esc(ago(S.sync.at))}</dd><dt>Waiting to sync</dt><dd>${n}</dd><dt>This phone</dt><dd>${esc(pname(null,c.me))}</dd><dt>Data</dt><dd>${esc(c.owner+'/'+c.repo)}</dd><dt>App version</dt><dd>${APP_VERSION}</dd></dl>
   <div class="acts"><button class="btn" id="sNow">Sync now</button><button class="btn ghost" id="sKey">Replace access key</button><button class="btn ghost" id="sOut">Disconnect this phone</button></div>
   <p class="note">Disconnecting removes the trips from this phone. Anything not yet synced is lost.</p></div>`);
  $('#sNow').onclick=()=>{closeSheet();sync(true)};$('#sKey').onclick=()=>{closeSheet();renderSetup(true)};
  $('#sOut').onclick=()=>confirmBox('Remove all trip data from this phone?','Disconnect',async()=>{await IDB.clear();LS.del('tl_cfg');LS.del('tl_lastsync');LS.del('tl_cred');location.hash='';location.reload()})}
function renderSetup(replace){setFab(null);$('#top').innerHTML='';$('#head').innerHTML='';const c=S.cfg||{owner:'clk1408-byte',repo:'trip-ledger-data',me:'a'};
  $('#app').innerHTML=`<div class="setup"><h1>${replace?'Replace access key':'Welcome to Jetsetr'}</h1><p class="lede">Connect this phone to your trips on GitHub. You only do this once.</p>
   <label class="f">Who’s using this phone?</label>${seg('me',[['a','Christian'],['b','Tami']],c.me||'a')}
   <label class="f">GitHub account<input id="sO" value="${esc(c.owner)}" autocapitalize="off" autocomplete="off"></label>
   <label class="f">Data repository<input id="sR" value="${esc(c.repo)}" autocapitalize="off" autocomplete="off"></label>
   <label class="f">Access key<input id="sT" type="password" placeholder="github_pat_…" autocomplete="off"></label>
   <div class="acts"><button class="btn" id="sGo">Connect</button>${replace?'<button class="btn ghost" id="sBack">Cancel</button>':''}</div><p class="note" id="sMsg"></p></div>`;
  const root=$('#app');wireSeg(root);addPeek(root);const tk=$('#sT');tk.setAttribute('autocorrect','off');tk.setAttribute('spellcheck','false');tk.addEventListener('input',()=>{const v=tk.value.trim();$('#sMsg').textContent=v?`${v.length} characters${/^github_pat_/.test(v)?'':' · keys normally start with github_pat_'}`:''});const b=$('#sBack');if(b)b.onclick=()=>route();
  $('#sGo').onclick=async()=>{const cfg={owner:$('#sO').value.trim(),repo:$('#sR').value.trim(),token:$('#sT').value.trim()||(replace&&S.cfg?S.cfg.token:''),me:segVal(root,'me')||'a'};
    const msg=$('#sMsg');if(!cfg.token)return msg.textContent='Paste the access key first.';msg.textContent='Checking…';
    try{const prev=S.cfg;S.cfg=cfg;const r=await gh(`${R()}`);cfg.branch=r.json.default_branch||'main';if(r.json.permissions&&r.json.permissions.push===false){S.cfg=prev;return msg.textContent='That key can read the repository but not save to it. Give it “Contents: Read and write”.'}
      LS.set('tl_cfg',cfg);S.cfg=cfg;location.hash='trips';route();sync(true)}
    catch(e){S.cfg=replace?S.cfg:null;msg.textContent=e.status===401?'GitHub didn’t accept that key. Check you copied all of it.':e.status===404?'Can’t see that repository. Check the name, and that the key has access to it.':'Couldn’t reach GitHub: '+e.message}}}

/* ================= boot ================= */
async function boot(){try{const st=await IDB.get('kv','state');if(st)Object.assign(S,{files:st.files||{},pending:st.pending||{},upQueue:st.upQueue||{},remoteBlobs:st.remoteBlobs||{},headSha:st.headSha||null})}catch(e){toast('This phone’s storage isn’t available: '+e.message,6000)}
  rebuild();route();if(S.cfg)sync();
  if('serviceWorker'in navigator){const had=!!navigator.serviceWorker.controller;let done=false;navigator.serviceWorker.addEventListener('controllerchange',()=>{if(had&&!done){done=true;location.reload()}});navigator.serviceWorker.register('sw.js').then(r=>r.update()).catch(()=>{})}
  if(navigator.storage&&navigator.storage.persist)navigator.storage.persist().catch(()=>{})}
boot();
}
