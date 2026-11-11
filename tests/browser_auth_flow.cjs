// Real navigation through the shipped UI and its response headers.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
(async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
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
    assert.match(await page.textContent('body'), /Iniciar sesi/);
    console.log('PASS: browser form origin preserved; login reaches validation; logout redirects');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
