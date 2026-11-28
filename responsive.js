/* My Daily — cross-device sync safety net (desktop ⇄ phone, same account).
   Realtime does the instant updates; this re-pulls when the window regains focus,
   when a tab is restored from the back/forward cache, and every 60s while visible. */
(function(){
  let busy=false;
  const typing=()=>{const a=document.activeElement;return !!(a&&/^(INPUT|TEXTAREA|SELECT)$/.test(a.tagName)&&a.closest&&a.closest('#app'))};
  async function pull(){
    if(busy||document.hidden||!navigator.onLine)return;
    try{
      if(typeof session==='undefined'||!session)return;
      if(typeof OFFLINE!=='undefined'&&OFFLINE)return;
      const dlg=document.getElementById('dlg');if(dlg&&dlg.open)return;
      if(typing())return;
      busy=true;await loadData();
      if(typeof tab==='undefined'||tab!=='set')render();
    }catch(e){}finally{busy=false}
  }
  addEventListener('focus',pull);
  addEventListener('pageshow',e=>{if(e.persisted)pull()});
  setInterval(pull,60000);
})();
