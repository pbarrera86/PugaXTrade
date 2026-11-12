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
    const dismissNotices = async () => {
      for (const close of await page.locator('.shiny-notification-close').all()) await close.click();
    };
    const checkTickerValidation = async (field, button) => {
      for (const [value, message] of [
        ['', 'Ingresa al menos un ticker válido.'],
        [Array.from({ length: 1501 }, (_, i) => `X${i}`).join(','), 'El máximo por análisis es de 1500 símbolos.'],
      ]) {
        await dismissNotices();
        await field.fill(value);
        await button.click();
        await page.getByText(message, { exact: true }).waitFor({ state: 'visible', timeout: 15000 });
        assert.equal(await button.isEnabled(), true, 'rejected requests must not start a background task');
      }
    };
    await checkTickerValidation(page.getByLabel('Tickers', { exact: true }), page.getByRole('button', { name: /Iniciar análisis$/ }));
    await page.getByRole('link', { name: 'Extremos Locales', exact: true }).click();
    await checkTickerValidation(page.getByLabel('Tickers a analizar (puedes escribir o pegar los que quieras)', { exact: true }), page.getByRole('button', { name: /Buscar empresas en MÍNIMOS locales/ }));
    console.log('PASS: authenticated analysis and extremes validation; empty and oversized input rejected before jobs');
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByLabel('Tickers a analizar (puedes escribir o pegar los que quieras)', { exact: true }).fill('AAPL');
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1), 'mobile analysis must fit the viewport');
    await page.emulateMedia({ reducedMotion: 'reduce' });
    assert.equal(await page.evaluate(() => matchMedia('(prefers-reduced-motion: reduce)').matches), true);
    await page.setViewportSize({ width: 1280, height: 800 });
    console.log('PASS: 390px mobile viewport and reduced-motion preference');
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
