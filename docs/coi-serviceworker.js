// GitHub Pages cannot set COOP/COEP headers. The same-origin service worker
// applies them to this site's responses so the shared WASM artifact can run.
// COEP is require-corp because WebKit, and so every iPhone browser, has no
// credentialless mode. require-corp blocks opaque responses and WebKit enforces
// each response's own CORP header, so cross-origin subresources load through CORS
// and every response is marked as embeddable.
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',event=>event.waitUntil(self.clients.claim()));
self.addEventListener('fetch',event=>{
 const {request}=event;
 if(request.cache==='only-if-cached' && request.mode!=='same-origin')return;
 const crossOrigin=request.mode==='no-cors' && new URL(request.url).origin!==self.location.origin;
 event.respondWith(fetch(crossOrigin?new Request(request.url,{mode:'cors',credentials:'omit'}):request).then(response=>{
  if(response.status===0)return response;
  const headers=new Headers(response.headers);
  headers.set('Cross-Origin-Opener-Policy','same-origin');
  headers.set('Cross-Origin-Embedder-Policy','require-corp');
  headers.set('Cross-Origin-Resource-Policy','cross-origin');
  return new Response(response.body,{status:response.status,statusText:response.statusText,headers});
 }));
});
