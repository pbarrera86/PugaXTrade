const {chromium} = require('playwright');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const assert = require('node:assert/strict');
(async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    const page=await browser.newPage();
    const dialogs=[];
    page.on('dialog',async d=>{dialogs.push(d.message());await d.dismiss();});
    await page.goto('http://localhost:3838/');
    await page.getByRole('button',{name:'Iniciar sesión',exact:true}).click();
    await page.getByLabel('Usuario o correo',{exact:true}).fill('ci_browser_fixture');
    await page.getByLabel('Contraseña',{exact:true}).fill('Browser fixture password 2026');
    await Promise.all([page.waitForNavigation(),page.getByRole('button',{name:'Entrar',exact:true}).click()]);
    await page.getByText(/Proceso concluido con éxito\. 500 tickers/).waitFor({timeout:30000});
    const table=page.locator('#main-inst-summary_tbl');
    let ajax=0;
    page.on('request',r=>{if(r.url().includes('/dataobj/')&&!r.url().includes('restore-auth'))ajax++;});
    await table.getByRole('combobox',{name:'Show entries',exact:true}).selectOption('100');
    await table.getByText('Showing 1 to 100 of 500 entries',{exact:true}).waitFor();
    await table.getByRole('link',{name:'Next',exact:true}).or(table.getByRole('button',{name:'Next',exact:true})).click();
    await table.getByText('Showing 101 to 200 of 500 entries',{exact:true}).waitFor();
    await table.getByLabel('Search:',{exact:true}).fill('FIX00042');
    await table.getByText('FIX00042',{exact:true}).waitFor();
    assert.equal(ajax,0,'paging and filtering must not require DT Ajax requests');
    await page.getByRole('button',{name:/Exportar Excel$/}).click();
    await page.getByText(/Preparando Excel en segundo plano/).waitFor();
    execFileSync('docker',['exec','--user','shiny','-e','PUGA_TEST_DATABASE=local-disposable','-e','PUGA_PREPARE_FIXTURE_EXPORT=true','-w','/srv/shiny-server/app','puga-ci','Rscript','tests/browser_result_seed.R'],{stdio:'inherit'});
    const [download]=await Promise.all([page.waitForEvent('download'),page.getByRole('link',{name:/Descargar Excel$/}).click()]);
    await download.saveAs('/tmp/puga-browser-export.xlsx');
    assert.equal(await download.failure(),null);
    const bytes=fs.readFileSync('/tmp/puga-browser-export.xlsx');
    assert.equal(bytes.subarray(0,2).toString(),'PK');assert.ok(bytes.length>1000);
    assert.equal(dialogs.length,0,'no DataTables warning may appear');
    console.log('PASS: client-side paging/filtering, asynchronous export status and authenticated XLSX download');
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
