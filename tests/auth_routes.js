const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync('www/script.js', 'utf8').split('Shiny.addCustomMessageHandler')[0];
for (const action of ['set-session', 'logout']) {
  test(`browser ${action} posts to dynamic auth prefix`, () => {
    let submitted;
    const document = {
      createElement: () => ({ children: [], appendChild(child) { this.children.push(child); }, submit() { submitted = this; } }),
      body: { appendChild() {} }
    };
    const context = vm.createContext({ document });
    vm.runInContext(source, context);
    context.pugaAuthPost(action, action === 'set-session' ? { code: 'one-use-fixture' } : {});
    assert.equal(submitted.method, 'POST');
    assert.equal(submitted.action, `/auth/${action}`);
    if (action === 'set-session') assert.equal(submitted.children[0].value, 'one-use-fixture');
  });
}

// Shiny rejects handlers unless they declare exactly one argument. A rejected
// registration aborts the rest of the shipped script, including reconnection.
test('all shipped custom message handlers satisfy Shiny registration', () => {
  const handlers = {};
  const script = fs.readFileSync('www/script.js', 'utf8').split('// ── Conexión estable')[0];
  vm.runInNewContext(script, { Shiny: { addCustomMessageHandler(name, handler) {
    assert.equal(handler.length, 1, name);
    handlers[name] = handler;
  } } });
  assert.ok(handlers.clearSessionCookie);
  assert.ok(handlers.restoreHttpOnlySession);
});
