// Run: node tests/unit_machine_inventory.js (no browser or network required).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new Function(script);
const code = script.slice(script.indexOf('let machineInventory ='), script.indexOf('function flashRunner('));
const nodes = new Map();
const node = id => {
  if (!nodes.has(id)) nodes.set(id, {textContent: '', innerHTML: '',
    insertAdjacentHTML(_, value) { this.innerHTML = value + this.innerHTML; }});
  return nodes.get(id);
};
const context = vm.createContext({$: node,
  esc: s => String(s).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;')});
vm.runInContext(code, context);
const machines = [
  {name: 'mac', host: '192.168.1.2', port: 22, user: 'a', repo: 'org/repo'},
  {name: 'mac-2', host: '192.168.1.2', port: 22, user: 'b', repo: 'org/repo'},
  {name: 'worker', host: '192.168.1.3', port: 22, user: 'a', pure: true},
];
assert.equal(context.groupMachines(machines).length, 2, 'same SSH endpoint counts once');
assert.equal(context.groupMachines([]).length, 0);
assert.equal(context.groupMachines([machines[0], {...machines[0], port:2222}]).length, 2);
assert.doesNotMatch(html, /待关联|id="runnerRepo"|id="sshRepo"|\/github\/runners/, 'machine page no longer queries or manages repository runners');
(async () => {
  context.fetch = async url => {
    assert.equal(url, '/runners');
    return {ok:true, json:async () => ({machines})};
  };
  await context.loadMachines(false);
  assert.equal(node('machineCount').textContent, '2 台');
  assert.match(node('machines').innerHTML, /192.168.1.2/);
  assert.doesNotMatch(node('machines').innerHTML, /org\/repo|常驻|关联/);
  context.fetch = async () => { throw new Error('offline'); };
  await context.loadMachines(false);
  assert.match(node('machines').innerHTML, /offline/);
  assert.match(node('machines').innerHTML, /192.168.1.2/, 'failed refresh retains existing machines');
  context.fetch = async () => ({ok:true, json:async () => ({machines:[]})});
  await context.loadMachines(false);
  assert.equal(node('machineCount').textContent, '0 台');
  assert.match(node('machines').innerHTML, /还没有接入机器/);
  console.log('PASS: endpoint deduplication, machine-only inventory, refresh errors and empty state');
})().catch(error => { console.error(error); process.exitCode = 1; });
