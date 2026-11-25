/* Trip Ledger service worker: the app opens instantly from the phone, with or without signal. */
const V='tl-1.0.2';
const SHELL=['./','./index.html','./app.js','./manifest.webmanifest','./icon.svg','./icon-192.png','./icon-512.png'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(V).then(c=>c.addAll(SHELL.map(u=>new Request(u,{cache:'reload'})))).then(()=>self.skipWaiting()))});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==V&&k!=='tl-img').map(k=>caches.delete(k)))).then(()=>self.clients.claim()))});
self.addEventListener('fetch',e=>{const r=e.request;if(r.method!=='GET')return;const u=new URL(r.url);
  if(u.hostname==='api.github.com'||u.hostname==='commons.wikimedia.org')return; /* always live */
  if(u.hostname==='upload.wikimedia.org'){e.respondWith(caches.open('tl-img').then(async c=>{const hit=await c.match(r);if(hit)return hit;const res=await fetch(r);if(res.ok||res.type==='opaque')c.put(r,res.clone());return res}));return}
  /* app files and fonts: answer from the phone at once, refresh in the background for next launch */
  e.respondWith(caches.open(V).then(async c=>{const hit=await c.match(r,{ignoreSearch:u.origin===location.origin});
    const net=fetch(r).then(res=>{if(res.ok||res.type==='opaque')c.put(r,res.clone());return res}).catch(()=>null);
    if(hit){e.waitUntil(net);return hit}const res=await net;if(res)return res;if(r.mode==='navigate'){const idx=await c.match('./index.html');if(idx)return idx}return new Response('Offline',{status:503})}))});
