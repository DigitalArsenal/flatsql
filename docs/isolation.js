// GitHub Pages cannot set COOP/COEP headers, so coi-serviceworker.js adds them,
// and a page is only isolated once it has been reloaded under that worker. Each
// tab reloads once: a browser still not isolated after that stays where it is,
// because another reload cannot help it. flatsqlIsolation resolves to whether
// this page is isolated, and stays pending while a reload is on its way.
(() => {
  const worker = new URL('coi-serviceworker.js', document.currentScript.src).href;
  // Per worker: this origin hosts other sites, and sessionStorage spans them.
  const RELOADED = `isolation-reloaded ${worker}`;

  const activated = (installing) => new Promise((resolve, reject) => {
    installing.addEventListener('statechange', () => {
      if (installing.state === 'activated') resolve();
      if (installing.state === 'redundant') reject(new Error('The isolation worker did not install'));
    });
  });

  async function isolate() {
    if (self.crossOriginIsolated) {
      sessionStorage.removeItem(RELOADED);
      return true;
    }
    if (!('serviceWorker' in navigator) || sessionStorage.getItem(RELOADED)) return false;
    const registration = await navigator.serviceWorker.register(worker);
    // A returning visitor may hold an older worker. The one reload must land on this one.
    await registration.update();
    const installing = registration.installing ?? registration.waiting;
    if (installing) await activated(installing);
    sessionStorage.setItem(RELOADED, '1');
    location.reload();
    return new Promise(() => {});
  }

  // A frame is isolated only if its top page is, and that page's reload carries the frame.
  const parentIsolation = () => {
    try {
      return parent !== self && parent.flatsqlIsolation;
    } catch {
      return null;
    }
  };

  self.flatsqlIsolation = parentIsolation() || isolate().catch((error) => {
    console.error('Browser isolation setup failed', error);
    return self.crossOriginIsolated === true;
  });
})();
