// Real navigation through the shipped UI and its response headers.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    page.on('response', async response => {
      if (response.url().includes('/dataobj/restore-auth')) {
        const headers = await response.request().allHeaders();
        console.log('Session restoration:', { status: response.status(), cookiePresent: (headers.cookie || '').includes('puga_auth='), origin: headers.origin });
      }
    });
    page.on('pageerror', error => console.log('Browser error:', error.message));
    let response = await page.goto('http://localhost:3838/');
    assert.equal(response.headers()['referrer-policy'], 'strict-origin');
    await page.waitForFunction(() => typeof window.pugaAuthPost === 'function');
    const navigation = page.waitForNavigation();
    await page.evaluate(() => window.pugaAuthPost('set-session', { code: 'expired-browser-fixture' }));
    response = await navigation;
    assert.equal(response.status(), 401, 'same-origin form must reach handoff validation, not return Forbidden');
    assert.match(await page.textContent('body'), /Inicia sesi/);
    await page.goto('http://localhost:3838/');
    await page.waitForFunction(() => typeof window.pugaAuthPost === 'function');
    const logoutNavigation = page.waitForNavigation();
    await page.evaluate(() => window.pugaAuthPost('logout', {}));
    await logoutNavigation;
    assert.equal(page.url(), 'http://localhost:3838/');
    await page.getByRole('button', { name: 'Iniciar sesión', exact: true }).waitFor({ state: 'visible', timeout: 15000 });
    assert.match(await page.textContent('body'), /Iniciar sesi/);
    await page.getByRole('button', { name: 'Iniciar sesión', exact: true }).click();
    await page.getByLabel('Usuario o correo', { exact: true }).fill('ci_browser_fixture');
    await page.getByLabel('Contraseña', { exact: true }).fill('Browser fixture password 2026');
    await Promise.all([
      page.waitForNavigation(),
      page.getByRole('button', { name: 'Entrar', exact: true }).click(),
    ]);
    const signedIn = page.getByRole('button', { name: /Cerrar sesión$/ });
    await signedIn.waitFor({ state: 'visible', timeout: 30000 }).catch(async error => { console.log('Login page state:', (await page.textContent('body')).slice(0, 1200)); throw error; });
    const cookie = (await page.context().cookies()).find(item => item.name === 'puga_auth');
    assert.ok(cookie, 'successful password login must create the session cookie');
    assert.equal(cookie.httpOnly, true);
    assert.equal(cookie.secure, true);
    assert.equal(cookie.sameSite, 'Lax');
    assert.equal(await page.evaluate(() => document.cookie.includes('puga_auth=')), false);
    assert.equal(page.url(), 'http://localhost:3838/');
    await page.reload();
    await signedIn.waitFor({ state: 'visible', timeout: 30000 });
    await page.getByRole('button', { name: 'Análisis', exact: true }).click();
    await page.getByLabel('Tickers', { exact: true }).fill('');
    await page.getByRole('button', { name: /Iniciar análisis$/ }).click();
    await page.getByText('Ingresa al menos un ticker válido.', { exact: true }).waitFor({ state: 'visible', timeout: 15000 });
    console.log('PASS: authenticated trial account can reach analysis validation');
    await Promise.all([page.waitForNavigation(), signedIn.click()]);
    await page.getByRole('button', { name: 'Iniciar sesión', exact: true }).waitFor({ state: 'visible', timeout: 15000 });
    assert.equal((await page.context().cookies()).some(item => item.name === 'puga_auth'), false);
    await page.reload();
    await page.getByRole('button', { name: 'Iniciar sesión', exact: true }).waitFor({ state: 'visible', timeout: 15000 });
    console.log('PASS: browser login, HttpOnly cookie, reload persistence, logout, and origin validation');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
