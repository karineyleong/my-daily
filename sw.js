const C='my-daily-v18';
const SHELL=['./','index.html','manifest.json','icon-180.png','icon-192.png','icon-512.png','supabase.js','offline.js'];
const CDN=['fonts.googleapis.com','fonts.gstatic.com'];
const OLD=/<script src="https:\/\/cdn\.jsdelivr\.net\/npm\/@supabase\/supabase-js@[^"]*"><\/script>/;
const NEW='<script src="supabase.js"></script><script src="offline.js"></script>';
const isPage=(r,u)=>r.mode==='navigate'||u.pathname.endsWith('/')||u.pathname.endsWith('/index.html');
async function patch(res){
  if(!res||!res.ok)return res;
  const t=await res.clone().text();
  if(!OLD.test(t))return res;
  return new Response(t.replace(OLD,NEW),{status:200,headers:{'Content-Type':'text/html; charset=utf-8'}});
}
self.addEventListener('install',e=>e.waitUntil(caches.open(C).then(c=>Promise.all(SHELL.map(u=>c.add(u).catch(()=>{})))).then(()=>self.skipWaiting())));
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==C).map(k=>caches.delete(k)))).then(()=>clients.claim())));
self.addEventListener('fetch',e=>{
  const r=e.request;if(r.method!=='GET')return;
  const u=new URL(r.url);
  if(u.origin===location.origin){
    const page=isPage(r,u);
    e.respondWith(fetch(r).then(res=>{if(res.ok){const c=res.clone();caches.open(C).then(x=>x.put(r,c))}return page?patch(res):res})
      .catch(()=>caches.match(r,{ignoreSearch:true}).then(m=>m||(page?caches.match('index.html'):undefined)).then(m=>m?(page?patch(m):m):Response.error())));
  }else if(CDN.includes(u.hostname)){
    e.respondWith(caches.match(r).then(m=>m||fetch(r).then(res=>{const c=res.clone();caches.open(C).then(x=>x.put(r,c));return res})));
  }
});
self.addEventListener('notificationclick',e=>{e.notification.close();e.waitUntil(clients.matchAll({type:'window'}).then(l=>l.length?l[0].focus():clients.openWindow('./')))});
