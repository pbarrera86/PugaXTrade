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
