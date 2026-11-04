// Small interactive smoke. Does not own or stop the live stack; no synthetic fallback.
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const [run, source, sampleFile] = process.argv.slice(2);
const { chromium, expect } = createRequire(path.join(source, 'apps/operations-web/package.json'))('@playwright/test');
const seed = JSON.parse(fs.readFileSync(sampleFile || path.join(run, 'seed.json'), 'utf8'));
const origin = 'http://127.0.0.1:3000';
const proof = { at: new Date().toISOString(), fixtureMode: false, mode: 'Preview', checks: [], orderId: seed.orderId };
const delay = ms => new Promise(r => setTimeout(r, ms));
(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  try {
    const page = await browser.newPage();
    const urls = [];
    page.on('request', request => urls.push(request.url()));
    await page.goto(origin);
    await expect(page.getByText('NO M36 AUTH', { exact: true })).toBeVisible();
    await expect(page.getByText('SYNTHETIC DEMO', { exact: true })).toHaveCount(0);
    const overview = await page.request.get(origin + '/api/ops/overview');
    if (overview.status() !== 200 || !(await overview.json()).asOf) throw Error('Live BFF overview failed');
    proof.checks.push('NEXT_LIVE_NO_M36_AUTH', 'BFF_LIVE_OVERVIEW');
    async function search(kind, owner, scope, title, ids, state) {
      await page.goto(origin + '/' + kind);
      const panel = title ? page.getByRole('heading', { name: title, exact: true })
        .locator("xpath=ancestor::*[.//button[normalize-space()='Search']][1]") : page;
      await panel.getByLabel('Environment').fill('test');
      await panel.getByLabel('Owner').fill(owner);
      await panel.getByLabel('Scope').fill(scope);
      await panel.getByLabel('From (UTC Z)').fill(new Date(seed.windowFrom).toISOString().replace(/\.\d{3}Z$/, 'Z'));
      await panel.getByLabel('To (UTC Z)').fill(new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'));
      // Explicit finite, rate-spaced retries only for declared source admission/backoff.
      let data;
      for (let i = 0; i < 3; i++) {
        await delay(2300);
        const response = page.waitForResponse(r => r.url().includes('/api/ops/recent/' + kind) && r.request().method() === 'GET');
        await panel.getByRole('button', { name: 'Search', exact: true }).click();
        const res = await response;
        if (res.status() !== 200) throw Error('BFF search HTTP=' + res.status());
        data = await res.json();
        if (data.availability === 'AVAILABLE' && data.freshness === 'FRESH') break;
        if (!['SHED', 'BACKOFF', 'BUSY', 'UNAVAILABLE'].includes(data.availability)) break;
      }
      if (data?.availability !== 'AVAILABLE' || !data.page?.coverage?.length) throw Error('Source not available: ' + owner + '/' + scope + ' ' + data?.availability);
      for (const id of ids) await expect(panel.getByText(id, { exact: true }).first()).toBeVisible({ timeout: 15000 });
      if (state) await expect(panel.getByText(state, { exact: true }).first()).toBeVisible();
      fs.writeFileSync(path.join(run, `smoke-${kind}-${owner}.json`), JSON.stringify(data, null, 2));
      proof.checks.push(kind + '/' + owner + '/' + scope);
    }
    for (const owner of ['order-cell', 'inventory-cell', 'payment-cell'])
      await search('events', owner, 'owner', null, seed.eventIds[owner], null);
    await search('workflows', 'order-cell', 'order', 'Order workflows', [seed.orderId], 'CONFIRMED');
    await search('workflows', 'inventory-cell', 'reservation', 'Inventory reservations', [seed.orderId], 'RESERVED');
    await search('workflows', 'payment-cell', 'authorization-simulation', 'Payment authorization simulation', [seed.orderId], 'AUTHORIZED');
    await search('inbox', 'inventory-cell', seed.inboxGroup, null, seed.inventoryInboxEventIds, null);
    await page.goto(origin + '/cells');
    await expect(page.getByText('order-cell').first()).toBeVisible();
    if ((await page.request.post(origin + '/api/ops/overview')).status() !== 405) throw Error('Mutation accepted');
    if ((await page.request.put(origin + '/api/ops/recent/events')).status() !== 405) throw Error('Mutation accepted');
    const bad = urls.filter(url => /^https?:/.test(url) && !url.startsWith(origin + '/'));
    if (bad.length) throw Error('Browser made non-BFF upstream request');
    proof.checks.push('CELLS', 'MUTATIONS_405', 'BROWSER_ONLY_NEXT_ORIGIN');
    await page.screenshot({ path: path.join(run, 'cells-live.png'), fullPage: true });
    proof.verdict = 'INTERACTIVE_DEMO_PASS';
  } finally {
    await browser.close();
    fs.writeFileSync(path.join(run, 'smoke.json'), JSON.stringify(proof, null, 2));
  }
})().catch(e => { console.error('Interactive smoke failed:', e.message); process.exitCode = 1; });
