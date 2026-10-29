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
    if(tab==='set'){const top=app.querySelector('.top');if(top&&top.previousElementSibling)top.previousElementSibling.insertAdjacentHTML('beforebegin',syncCard()+bgCard()+notifCard())}};
  LS.removeItem('gk');
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
  setInterval(checkReminders,30000);checkReminders();extra();
}
function extra(){
  T.zh.todoT='待辦清單';T.ja.todoT='ToDoリスト';T.en.todoT='To-do list';
  T.zh.brainstorm='計劃表';T.ja.brainstorm='計画表';T.en.brainstorm='Plans';
  const st=document.createElement('style');
  st.textContent='.cg .cd.hol .dn{color:#D0483E}.cg .cd.td.hol .dn{color:var(--on)}.chp.hol{background:#D0483E}.cm b{cursor:pointer;text-decoration:underline dotted}';
  document.head.appendChild(st);
  /* keyboard-aware bottom sheet */
  const vv=window.visualViewport,dl=$('dlg');let moved=false;
  const fit=()=>{if(!vv||!dl.open)return;const kb=Math.max(0,innerHeight-vv.height-vv.offsetTop);
    if(kb>60){moved=true;dl.style.marginBottom=kb+'px';dl.style.maxHeight=Math.round(vv.height*.92)+'px'}
    else if(moved){moved=false;dl.style.marginBottom='';dl.style.maxHeight='60dvh'}};
  if(vv){vv.addEventListener('resize',fit);vv.addEventListener('scroll',fit)}
  dl.addEventListener('focusin',e=>setTimeout(()=>{fit();try{e.target.scrollIntoView({block:'center',behavior:'smooth'})}catch(_){}},350));
  dl.addEventListener('close',()=>{moved=false;dl.style.marginBottom=''});
  /* Japanese public holidays */
  const HN=['元日|元旦|New Year’s Day','成人の日|成人日|Coming of Age Day','建国記念の日|建國紀念日|Foundation Day','天皇誕生日|天皇誕辰|Emperor’s Birthday','春分の日|春分|Vernal Equinox','昭和の日|昭和之日|Showa Day','憲法記念日|憲法紀念日|Constitution Day','みどりの日|綠之日|Greenery Day','こどもの日|兒童節|Children’s Day','海の日|海之日|Marine Day','山の日|山之日|Mountain Day','敬老の日|敬老日|Respect for the Aged Day','秋分の日|秋分|Autumnal Equinox','スポーツの日|體育日|Sports Day','文化の日|文化日|Culture Day','勤労感謝の日|勤勞感謝日|Labor Thanksgiving Day','振替休日|補假|Substitute Holiday','国民の休日|國民假日|Citizens’ Holiday'];
  const HC={};
  const jpHol=y=>{if(HC[y])return HC[y];const h={},k=(m,d,i)=>{h[m+'-'+d]=i},nm=(m,n)=>1+((8-new Date(y,m-1,1).getDay())%7)+7*(n-1),q=y-1980,fl=Math.floor;
    k(1,1,0);if(y>=2000)k(1,nm(1,2),1);if(y>=1967)k(2,11,2);if(y>=2020)k(2,23,3);k(3,fl(20.8431+.242194*q-fl(q/4)),4);if(y>=2007)k(4,29,5);k(5,3,6);if(y>=2007)k(5,4,7);k(5,5,8);
    if(y>=2003)k(7,y==2020?23:y==2021?22:nm(7,3),9);if(y>=2016)k(8,y==2020?10:y==2021?8:11,10);if(y>=2003)k(9,nm(9,3),11);k(9,fl(23.2488+.242194*q-fl(q/4)),12);
    if(y>=2020){if(y==2020)k(7,24,13);else if(y==2021)k(7,23,13);else k(10,nm(10,2),13)}k(11,3,14);k(11,23,15);
    Object.keys(h).forEach(key=>{const[m,d]=key.split('-').map(Number);if(new Date(y,m-1,d).getDay()===0){const t=new Date(y,m-1,d+1);while(h[(t.getMonth()+1)+'-'+t.getDate()])t.setDate(t.getDate()+1);h[(t.getMonth()+1)+'-'+t.getDate()]=16}});
    for(let m=1;m<=12;m++)for(let d=1;d<=31;d++){const dt=new Date(y,m-1,d);if(dt.getMonth()!==m-1||h[m+'-'+d]!=null||dt.getDay()===0)continue;const p=new Date(y,m-1,d-1),n=new Date(y,m-1,d+1);if(h[(p.getMonth()+1)+'-'+p.getDate()]!=null&&p.getFullYear()===y&&h[(n.getMonth()+1)+'-'+n.getDate()]!=null&&n.getFullYear()===y)h[m+'-'+d]=17}
    return HC[y]=h};
  const hname=dt=>{const i=jpHol(dt.getFullYear())[(dt.getMonth()+1)+'-'+dt.getDate()];return i==null?'':HN[i].split('|')[{ja:0,zh:1,en:2}[lang]]};
  /* year/month wheel picker (native iOS wheel) */
  const pickYM=()=>{const y=cm.getFullYear(),ys=[];for(let i=y-30;i<=y+30;i++)ys.push(i);
    dl.innerHTML=`<div class="tbar"><select id="py">${ys.map(v=>`<option value="${v}"${v===y?' selected':''}>${lang==='en'?v:v+'年'}</option>`).join('')}</select><select id="pm">${[...Array(12)].map((_,i)=>`<option value="${i}"${i===cm.getMonth()?' selected':''}>${new Date(2000,i,1).toLocaleDateString(LOC[lang],{month:'long'})}</option>`).join('')}</select></div><div class="tbar"><button class="pri" id="pk">✓</button></div>`;
    const ap=()=>{cm=new Date(+$('py').value,+$('pm').value,1);render()};$('py').onchange=ap;$('pm').onchange=ap;$('pk').onclick=()=>dl.close();dl.showModal()};
  /* side drawers for calendar (event categories) and habits */
  let sdo=false,lastTab=null;
  const dHead=t0=>`<div class="ov op" data-a="sd"></div><aside class="dr op"><div class="dh"><b>${t0}</b><button class="sec ic" data-a="sd" aria-label="✕">✕</button></div>`;
  const habDr=()=>dHead(t('habits'))+habits.filter(h=>!h.archived).map(h=>`<button class="di" data-a="hview" data-id="${h.id}"><span class="ic2">${esc(h.icon||'•')}</span><span class="nm">${esc(h.name)}</span></button>`).join('')+`<hr><button class="di" data-a="nhabit"><span class="ic2">＋</span><span class="nm">${t('newHabit')}</span></button>`+(habits.some(h=>h.archived)?`<hr><b>${t('arcSec')}</b>`+habits.filter(h=>h.archived).map(h=>`<div class="task"><div class="tt"><b>${esc((h.icon||'')+' '+h.name)}</b></div><button class="sec" data-a="hunarc" data-id="${h.id}">${t('unarc')}</button></div>`).join(''):'')+'</aside>';
  const _av2=appView;appView=function(){
    if(tab!==lastTab){sdo=false;lastTab=tab}if(hview)sdo=false;
    _av2();const app=$('app'),hd=app.querySelector('.hd');
    if(tab==='todo'&&view==='all'){const h=app.querySelector('.hd h1');if(h)h.textContent=t('todoT')}
    if((tab==='cal'||(tab==='habit'&&!hview))&&hd)hd.insertAdjacentHTML('afterbegin','<button class="sec ic" data-a="sd" aria-label="☰">☰</button>');
    if(tab==='cal'){
      const ev=app.querySelector('[data-a="nle"]'),evc=ev&&ev.closest('.card');if(evc)evc.remove();
      const b=app.querySelector('.cm b');if(b)b.dataset.a='cpick';
      app.querySelectorAll('.cd[data-v]').forEach(c=>{const m=c.dataset.v.match(/^(\d+)-(-?\d+)-(\d+)$/);if(!m)return;const n=hname(new Date(+m[1],+m[2],+m[3]));if(!n)return;c.classList.add('hol');const dn=c.querySelector('.dn');if(dn)dn.insertAdjacentHTML('afterend',`<span class="chp hol">${esc(n)}</span>`)});
      const hn=hname(cs),cards=app.querySelectorAll('.card');if(hn&&cards[1]){const bb=cards[1].querySelector('b');if(bb)bb.insertAdjacentText('beforeend',' 🎌 '+hn)}
      if(sdo&&evc)app.insertAdjacentHTML('beforeend',dHead(t('evCats'))+evc.outerHTML+'</aside>')}
    if(tab==='habit'&&!hview&&sdo)app.insertAdjacentHTML('beforeend',habDr())};
  document.addEventListener('click',e=>{const b=e.target.closest&&e.target.closest('[data-a="sd"],[data-a="cpick"]');if(!b)return;
    if(b.dataset.a==='sd'){sdo=!sdo;render()}else pickYM()},true);
}
})();
