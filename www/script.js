// ── PugaX Trade — script.js ──────────────────────────────────────────────────
// Las cookies de sesión se gestionan server-side (HttpOnly) via /auth/set-session
// JS nunca lee ni escribe la cookie puga_auth directamente.

// Redirige al endpoint server-side que establece la cookie con flag HttpOnly
Shiny.addCustomMessageHandler('setHttpOnlyCookie', function(x) {
  var base = window.location.origin;
  window.location.replace(base + '/?action=set-session&tok=' + encodeURIComponent(x.tok));
});

// Logout: redirige al endpoint que borra la cookie HttpOnly server-side
Shiny.addCustomMessageHandler('clearSessionCookie', function(x) {
  var base = window.location.origin;
  window.location.replace(base + '/?action=logout');
});

// reset_idle y otros handlers de UX
Shiny.addCustomMessageHandler('reset_idle', function(x) {
  // El timer de inactividad se reinicia en el lado R con shinyjs
});

// ── Conexión estable (keepalive + recuperación automática) ───────────────────
// El proxy (Traefik v3 en Dokploy) cierra las conexiones WebSocket cuando el
// navegador no envía nada durante ~60 s. Una página Shiny en reposo no envía
// nada, así que mandamos una señal ligera cada 20 s. El temporizador corre en
// un Web Worker para que Chrome no lo frene cuando la pestaña está en segundo plano.
(function () {
  var KEEPALIVE_MS = 20000;

  function ping() {
    try {
      if (window.Shiny && Shiny.setInputValue && Shiny.shinyapp && Shiny.shinyapp.isConnected()) {
        Shiny.setInputValue('pugax_keepalive', Date.now(), { priority: 'event' });
      }
    } catch (e) { /* sin acción */ }
  }

  function startKeepalive() {
    try {
      var src = 'setInterval(function(){ postMessage(1); }, ' + KEEPALIVE_MS + ');';
      var worker = new Worker(URL.createObjectURL(new Blob([src], { type: 'application/javascript' })));
      worker.onmessage = ping;
    } catch (e) {
      setInterval(ping, KEEPALIVE_MS); // respaldo si el navegador no permite workers
    }
  }

  // Recuperación: si la conexión se pierde de todos modos (internet, reinicio del
  // servidor, laptop suspendida), recarga la página en lugar de quedarse en
  // "Disconnected from the server". La sesión se conserva (cookie HttpOnly).
  var reloadTimer = null;
  function showOverlayMsg() {
    if (document.getElementById('pugax-reconnect')) return;
    var d = document.createElement('div');
    d.id = 'pugax-reconnect';
    d.style.cssText = 'position:fixed;top:16px;left:50%;transform:translateX(-50%);z-index:99999;background:#1f2937;color:#fff;' +
      'padding:12px 16px;border-radius:12px;box-shadow:0 4px 18px rgba(0,0,0,.4);font-weight:600;';
    d.innerHTML = '&#x21bb; Se perdió la conexión. Reconectando automáticamente...';
    document.body.appendChild(d);
  }
  function reloadNow() {
    var n = parseInt(sessionStorage.getItem('pugax_reloads') || '0', 10);
    if (n >= 3) return; // evita ciclos si el servidor está caído; el usuario puede recargar a mano
    sessionStorage.setItem('pugax_reloads', String(n + 1));
    window.location.reload();
  }
  function scheduleReload() {
    if (reloadTimer) return;
    showOverlayMsg();
    // Damos unos segundos a la reconexión nativa de Shiny antes de recargar
    reloadTimer = setTimeout(function () {
      if (window.Shiny && Shiny.shinyapp && Shiny.shinyapp.isConnected()) { reloadTimer = null; return; }
      if (document.visibilityState === 'visible') {
        reloadNow();
      } else {
        document.addEventListener('visibilitychange', function onVis() {
          if (document.visibilityState === 'visible') {
            document.removeEventListener('visibilitychange', onVis);
            reloadNow();
          }
        });
      }
    }, 6000);
  }

  $(document).on('shiny:connected', function () {
    sessionStorage.setItem('pugax_reloads', '0');
    var m = document.getElementById('pugax-reconnect');
    if (m) m.remove();
    if (reloadTimer) { clearTimeout(reloadTimer); reloadTimer = null; }
    if (!window.__pugaxKeepalive) { window.__pugaxKeepalive = true; startKeepalive(); }
  });
  $(document).on('shiny:disconnected', scheduleReload);
})();
