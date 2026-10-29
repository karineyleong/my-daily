/* My Daily — offline layer (v11). Load AFTER supabase.js and BEFORE the main inline script. */
(()=>{
const S=window.supabase;if(!S||!S.createClient)return;
const LS=localStorage,jp=(k,d)=>{try{const v=JSON.parse(LS[k]);return v==null?d:v}catch(_){return d}},sv=(k,v)=>{try{LS[k]=JSON.stringify(v)}catch(_){}};
const uuid=()=>crypto.randomUUID?crypto.randomUUID():'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,c=>{const r=Math.random()*16|0;return(c=='x'?r:r&3|8).toString(16)});
const isNet=e=>!!e&&(e.status===0||/fetch|network|load failed|timeout|offline/i.test(e.message||''));
const isAuth=e=>!!e&&(e.status===401||/jwt/i.test(e.message||''));
const tmo=(p,ms)=>Promise.race([p,new Promise(r=>setTimeout(()=>r({data:null,error:{message:'timeout',status:0}}),ms))]);
let Q=jp('md_q',[]),realFrom,authGet,flushing=false,loaded=false,pT,patched=false;
const uid=()=>{try{return session&&session.user&&session.user.id}catch(_){return null}};
const pseudo=()=>{try{return !!(session&&session.offline)}catch(_){return false}};
const offSess=()=>{const s=jp('md_sess',null);return s&&s.user?{user:s.user,offline:true}:null};

/* ---------- query builder: same chain API as supabase-js, queues writes when offline ---------- */
const build=sp=>{let q=realFrom(sp.t);
  if(sp.o==='select')q=q.select('*');else if(sp.o==='insert')q=q.insert(sp.p);else if(sp.o==='update')q=q.update(sp.p);else if(sp.o==='upsert')q=q.upsert(sp.p,sp.opt||undefined);else q=q.delete();
  sp.f.forEach(([k,c,v])=>{q=q[k](c,v)});sp.ord.forEach(([c,a])=>{q=q.order(c,a)});
  if(sp.rng)q=q.range(sp.rng[0],sp.rng[1]);
  if(sp.sel&&sp.o!=='select')q=q.select();
  if(sp.one==='single')q=q.single();else if(sp.one==='maybe')q=q.maybeSingle();
  return q};
class QB{constructor(t){this.sp={t,o:null,p:null,f:[],ord:[],sel:false,one:null,opt:null,rng:null}}
  select(){this.sp.sel=true;if(!this.sp.o)this.sp.o='select';return this}
  insert(v){this.sp.o='insert';this.sp.p=v;return this}
  update(v){this.sp.o='update';this.sp.p=v;return this}
  upsert(v,o){this.sp.o='upsert';this.sp.p=v;this.sp.opt=o;return this}
  delete(){this.sp.o='delete';return this}
  eq(c,v){this.sp.f.push(['eq',c,v]);return this}
  is(c,v){this.sp.f.push(['is',c,v]);return this}
  in(c,v){this.sp.f.push(['in',c,v]);return this}
  order(c,o){this.sp.ord.push([c,o]);return this}
  range(a,b){this.sp.rng=[a,b];return this}
  single(){this.sp.one='single';return this}
  maybeSingle(){this.sp.one='maybe';return this}
  then(a,b){return(this.sp.o==='select'?readQ(this.sp):writeQ(this.sp)).then(a,b)}}
async function readQ(sp){
  if(!navigator.onLine||pseudo())return{data:null,error:{message:'offline',status:0}};
  if(sp.one||sp.rng)return tmo(build(sp),12000);
  let out=[],from=0,addId=!sp.ord.length;
  for(;;){
    const r=await tmo(build({...sp,ord:addId?[['id',{ascending:true}]]:sp.ord,rng:[from,from+999]}),12000);
    if(r.error){if(addId&&r.error.code==='42703'){addId=false;continue}return r}
    out=out.concat(r.data);if(r.data.length<1000)break;from+=1000}
  return{data:out,error:null}}
async function writeQ(sp){
  const w={...sp,f:sp.f.slice()};
  if(w.o==='insert'){const add=r=>({...r,id:r.id||uuid()});w.p=Array.isArray(w.p)?w.p.map(add):add(w.p)}
  if(navigator.onLine&&!Q.length&&!pseudo()){const r=await tmo(build(w),8000);if(!isNet(r.error))return r}
  const{sel,one,...rest}=w;Q.push({...rest,u:uid()});sv('md_q',Q);setTimeout(()=>dispatchEvent(new Event('md-q')),0);
  const now=new Date().toISOString(),mk=r=>({...r,user_id:r.user_id||uid(),created_at:r.created_at||now,id:r.id||uuid()});
  let data=null;if(sel&&(w.o==='insert'||w.o==='upsert')){const rows=(Array.isArray(w.p)?w.p:[w.p]).map(mk);data=one?rows[0]:rows}
  return{data,error:null,status:w.o==='insert'?201:200}}
async function flush(){
  if(flushing||!Q.length||!navigator.onLine)return;flushing=true;
  try{const{data}=await authGet();if(!data||!data.session)return;const me=data.session.user.id;
    for(const op of Q.slice()){
      if(op.u&&op.u!==me)continue;
      const go=()=>tmo(build({...op,sel:false,one:null}),10000);
      let r=await go();
      if(r.error&&r.error.code==='22P02'&&op.o==='insert'){const st=x=>{const{id,...y}=x;return y};op.p=Array.isArray(op.p)?op.p.map(st):st(op.p);r=await go()}
      if(isNet(r.error))break;
      if(isAuth(r.error))break;
      Q.splice(Q.indexOf(op),1);sv('md_q',Q)}
  }finally{flushing=false;dispatchEvent(new Event('md-q'))}}

/* ---------- local cache of all data ---------- */
function persist(){if(!loaded||!uid()||pseudo()&&false)return;sv('md_cache',{u:uid(),tasks,cats,ideas,maps,habits,hlogs,events,ecats,curMap})}
function loadCache(){const c=jp('md_cache',null);
  if(c&&c.u===uid()){tasks=c.tasks||[];cats=c.cats||[];ideas=c.ideas||[];maps=c.maps||[];habits=c.habits||[];hlogs=c.hlogs||[];events=c.events||[];ecats=c.ecats||[];if(c.curMap&&!curMap)curMap=c.curMap;ideaErr=null;habitErr=null;evErr=null}
  loaded=true}

/* ---------- client wrapper: patches the client returned by createClient ---------- */
function patchClient(c){
  realFrom=c.from.bind(c);c.from=t=>new QB(t);
  const a=c.auth;authGet=a.getSession.bind(a);const oSO=a.signOut.bind(a),oOC=a.onAuthStateChange.bind(a);
  const needPseudo=async()=>{if(!offSess())return false;if(!navigator.onLine)return true;
    try{await fetch(c.supabaseUrl+'/auth/v1/health',{cache:'no-store',mode:'no-cors',signal:AbortSignal.timeout(3000)});return false}catch(_){return true}};
  a.getSession=async()=>{const r=await authGet();if(!(r.data&&r.data.session)&&await needPseudo())return{data:{session:offSess()},error:null};return r};
  a.onAuthStateChange=cb=>oOC(async(ev,s)=>{
    if(!patched){patched=true;applyPatches()}
    if(!s&&ev!=='SIGNED_OUT'&&await needPseudo())s=offSess();
    if(s&&!s.offline)sv('md_sess',{user:{id:s.user.id,email:s.user.email}});
    if(ev==='SIGNED_OUT')LS.removeItem('md_sess');
    return cb(ev,s)});
  a.signOut=async()=>{await flush();if(Q.length&&!confirm(t('unsynced')))return{error:null};
    try{await Promise.race([oSO(),new Promise(r=>setTimeout(r,2500))])}catch(_){}
    Object.keys(LS).filter(k=>k.startsWith('sb-')||k==='md_sess'||k==='md_cache').forEach(k=>LS.removeItem(k));
    location.reload();return{error:null}};
  return c}
window.supabase=Object.assign({},S,{createClient:(...x)=>patchClient(S.createClient(...x))});

/* ---------- UI pieces ---------- */
const L={syncT:['同步','同期','Sync'],online:['已連線','オンライン','Online'],offline:['離線中','オフライン','Offline'],pending:['待同步','同期待ち','Pending'],synced:['已全部同步 ✓','すべて同期済み ✓','All synced ✓'],syncNow:['立即同步','今すぐ同期','Sync now'],
unsynced:['還有變更尚未同步，現在登出會遺失這些資料。確定要登出嗎？','未同期の変更があります。ログアウトするとデータが失われます。続けますか？','Some changes are not synced yet and would be lost. Log out anyway?'],
needNet:['這個操作需要網絡連線。','この操作にはネット接続が必要です。','This needs an internet connection.'],copy:['複製','コピー','Copy'],notifT:['提醒通知','通知','Notifications'],
notifHint:['開啟後，App 開著（包括離線）時，行程會在提醒時間彈出通知。App 完全關閉時，iPhone 暫時無法提醒。','オンにすると、アプリを開いている間（オフライン含む）、予定の通知時刻に通知します。アプリを完全に閉じている間は通知できません。','While the app is open (even offline), events notify at their reminder time. iPhone cannot notify while the app is fully closed.'],
notifNo:['此瀏覽器不支援通知','このブラウザは通知に未対応です','Notifications not supported here'],notifOn:['通知已開啟','通知オン','Notifications on'],notifAsk:['開啟通知','通知をオンにする','Turn on notifications']};
const badge=()=>(navigator.onLine?'':'📴 '+t('offline'))+(Q.length?' ⏳'+Q.length:'');
const syncCard=()=>`<div class="card"><b>🔄 ${t('syncT')}</b><p class="meta">${navigator.onLine?t('online'):t('offline')} · ${Q.length?t('pending')+' '+Q.length:t('synced')}</p><div class="row"><button class="sec" data-a="sync">${t('syncNow')}</button></div></div>`;
const notifCard=()=>`<div class="card"><b>🔔 ${t('notifT')}</b><p class="meta">${t('notifHint')}</p><div class="row"><button class="sec" data-a="notif">${!window.Notification?t('notifNo'):Notification.permission==='granted'?'✓ '+t('notifOn'):t('notifAsk')}</button></div></div>`;
const planBar=()=>`<div class="pchips">${maps.map(m=>`<button class="pchip${m.id===curMap?' on':''}" data-a="map" data-id="${m.id}">${esc(m.name)}</button>`).join('')}<button class="pchip" data-a="nmap">＋ ${t('plan')}</button></div>`+(mapIdeas().length?`<div class="row" style="margin:0 0 10px"><button class="sec" data-a="p2t">✓ ${t('toTodo')}</button><button class="sec" data-a="cpy">📋 ${t('copy')}</button><button class="sec" data-a="mmrst">↺ ${t('posReset')}</button></div>`:'');
async function showN(title,body,tag){try{const r=await navigator.serviceWorker.ready;await r.showNotification(title,{body,tag,icon:'icon-192.png'})}catch(_){try{new Notification(title,{body})}catch(__){}}}
function checkReminders(){
  if(!session||!window.Notification||Notification.permission!=='granted')return;
  const fired=jp('md_fired',{}),now=Date.now(),td=new Date(),ds=ymd(td);
  events.forEach(e=>{
    if(e.remind_min==null||e.all_day||!e.start_at||!occursOn(e,td))return;
    const s=new Date(e.start_at);if((!e.repeat_kind||e.repeat_kind==='none')&&ymd(s)!==ds)return;
    const st=new Date(td.getFullYear(),td.getMonth(),td.getDate(),s.getHours(),s.getMinutes()),at=+st-e.remind_min*6e4,k=e.id+'|'+ds;
    if(now>=at&&now<at+10*6e4&&!fired[k]){fired[k]=1;showN(e.title||'',hm(st)+(e.location?' 📍'+e.location:''),k)}});
  const keep={};Object.keys(fired).forEach(k=>{if(k.endsWith('|'+ds))keep[k]=1});sv('md_fired',keep)}
function cacheBg(){if(!bgUrl||!bgPath){LS.removeItem('md_bgd');LS.removeItem('md_bgp');return}if(LS.md_bgp===bgPath&&LS.md_bgd)return;
  fetch(bgUrl).then(r=>r.blob()).then(b=>{const fr=new FileReader();fr.onload=()=>{try{LS.md_bgd=fr.result;LS.md_bgp=bgPath}catch(_){}};fr.readAsDataURL(b)}).catch(()=>{})}
async function syncNow(){if(!session)return;await flush();if(navigator.onLine&&!Q.length&&!pseudo()&&!$('dlg').open)await loadData();render()}

/* ---------- overrides of main-script functions (run once, after the main script is loaded) ---------- */
function applyPatches(){
  Object.keys(L).forEach(k=>['zh','ja','en'].forEach((l,i)=>{T[l][k]=L[k][i]}));
  T.zh.freq='執行日';T.zh.restToday='今日休息／尚未開始';
  const _av=appView;appView=function(){_av();const app=$('app'),ap=app.querySelector('.appt'),b=badge();
    if(ap&&b)ap.insertAdjacentHTML('beforeend',`<small style="font-size:12px;color:var(--mut)"> ${b}</small>`);
    if(toastMsg)app.insertAdjacentHTML('beforeend',`<div class="toast">${esc(toastMsg)}</div>`);
    if(tab==='set'){const top=app.querySelector('.top');if(top&&top.previousElementSibling)top.previousElementSibling.insertAdjacentHTML('beforebegin',syncCard()+bgCard()+gkCard()+notifCard())}};
  const _c=calHTML;calHTML=function(){return _c().replace(gkCard(),'')};
  const _i=ideaHTML;ideaHTML=function(){const h=_i();return ideaErr?h:h.replace('</div>','</div>'+planBar())};
  const _r=render;render=function(){clearTimeout(pT);pT=setTimeout(persist,150);return _r.apply(this,arguments)};
  const _ld=loadData;loadData=async function(){
    if(navigator.onLine&&!pseudo())await flush();
    if(!navigator.onLine||pseudo()||Q.length){loadCache();return}
    let failed=false;const oa=window.alert;window.alert=m=>{failed=true;if(!isNet({message:String(m)}))oa(m)};
    try{await _ld()}finally{window.alert=oa}
    if(failed){loadCache();return}
    loaded=true;persist()};
  const _ls=loadSettings;loadSettings=function(){return navigator.onLine&&!pseudo()?_ls():Promise.resolve()};
  const _lb=loadBg;loadBg=async function(){if(!navigator.onLine||pseudo()){bgPath=LS.md_bgp||'';bgUrl=LS.md_bgd||'';theme();return}await _lb();cacheBg()};
  const _sbp=setBgPath;setBgPath=async function(p){await _sbp(p);cacheBg()};
  const _ub=upBg;upBg=function(f){if(!navigator.onLine){alert(t('needNet'));return}return _ub(f)};
  document.addEventListener('click',async e=>{const b=e.target.closest&&e.target.closest('[data-a="sync"],[data-a="notif"]');if(!b)return;
    if(b.dataset.a==='sync'){await syncNow();say(Q.length?t('pending')+' '+Q.length:t('synced'))}
    else{if(window.Notification)await Notification.requestPermission();render()}},true);
  addEventListener('online',syncNow);
  addEventListener('offline',()=>{try{if(session)render()}catch(_){}});
  addEventListener('md-q',()=>{try{if(session&&rendered)render()}catch(_){}});
  addEventListener('pagehide',persist);
  document.addEventListener('visibilitychange',()=>{if(document.hidden)persist();else syncNow()});
  setInterval(()=>{if(navigator.onLine&&Q.length)syncNow()},30000);
  setInterval(checkReminders,30000);checkReminders();
}
})();
