// GitHub Pages cannot set COOP/COEP headers, so coi-serviceworker.js adds them,
// and a page is only isolated once it has been reloaded under that worker.
// flatsqlIsolation resolves once this page is isolated.
(() => {
  const worker = new URL('coi-serviceworker.js', document.currentScript.src).href;

  async function isolate() {
    if (self.crossOriginIsolated) return true;
    if (!('serviceWorker' in navigator)) throw new Error('This browser cannot isolate shared WASM memory');
    await navigator.serviceWorker.register(worker);
    await navigator.serviceWorker.ready;
    if (navigator.serviceWorker.controller) location.reload();
    else navigator.serviceWorker.addEventListener('controllerchange', () => location.reload(), { once: true });
    return new Promise(() => {});
  }

  self.flatsqlIsolation = isolate();
})();
