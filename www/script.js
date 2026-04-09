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
