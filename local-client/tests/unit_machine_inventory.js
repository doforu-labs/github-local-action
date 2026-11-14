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
  // 界面文案的中英文切换（t/L）是后来才加的，测试得给上桩，
  // 不然 renderMachineInventory 一调 L 就 ReferenceError，整套跑不起来
  t: zh => zh,
  L: (zh, _en) => zh,
  // 转圈图标和时间格式化也在切片外面（它们住在 job 卡片那段代码里），做个等价物
  SPIN: '<span class="spin"></span>',
  fmtFull: ts => String(ts || ''),
  // 机器页现在分两步加载（清单 + /machines/live 状态），状态到的时候
  // 只去改已有卡片上的标签。测试里没有真的 DOM，给个空查询就行。
  document: {querySelectorAll: () => []},
  esc: s => String(s).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;')});
vm.runInContext(code, context);
const machines = [
  {name: 'mac', host: '10.0.0.21', port: 22, user: 'a', repo: 'org/repo'},
  {name: 'mac-2', host: '10.0.0.21', port: 22, user: 'b', repo: 'org/repo'},
  {name: 'worker', host: '10.0.0.22', port: 22, user: 'a', pure: true},
];
assert.equal(context.groupMachines(machines).length, 2, 'same SSH endpoint counts once');
assert.equal(context.groupMachines([]).length, 0);
assert.equal(context.groupMachines([machines[0], {...machines[0], port:2222}]).length, 2);
assert.doesNotMatch(html, /待关联|id="runnerRepo"|id="sshRepo"|\/github\/runners/, 'machine page no longer queries or manages repository runners');
// Machine connectivity no longer depends on a repository registration.
for (const [state,label,error] of [
  ['idle','在线 · 待命',''], ['unprepared','在线 · 待准备',''],
  ['offline','连接失败','SSH 认证失败'], ['unknown','状态未知','探测失败'], ['busy','有任务在跑','']
]) {
  vm.runInContext('machineLive = ' + JSON.stringify({mac:{state,error}}), context);
  const status = context.machineLiveHtml({name:'mac'});
  assert.ok(status.includes(label), state);
  assert.doesNotMatch(status, /未注册|GitHub 上注册/);
  if (error) assert.ok(status.includes(error));
}
(async () => {
  context.fetch = async url => {
    if (url === '/runners') return {ok:true, json:async () => ({machines})};
    // 第二步问机器状态；这个测试只关心清单，状态给个空的
    if (url === '/machines/live') return {ok:true, json:async () => ({machines:{}, repos:[]})};
    throw new Error('unexpected request: ' + url);
  };
  await context.loadMachines(false);
  assert.equal(node('machineCount').textContent, '2 台');
  assert.match(node('machines').innerHTML, /10.0.0.21/);
  assert.doesNotMatch(node('machines').innerHTML, /org\/repo|常驻|关联/);
  context.fetch = async () => { throw new Error('offline'); };
  await context.loadMachines(false);
  assert.match(node('machines').innerHTML, /offline/);
  assert.match(node('machines').innerHTML, /10.0.0.21/, 'failed refresh retains existing machines');
  context.fetch = async () => ({ok:true, json:async () => ({machines:[]})});
  await context.loadMachines(false);
  assert.equal(node('machineCount').textContent, '0 台');
  assert.match(node('machines').innerHTML, /还没有接入机器/);
  console.log('PASS: endpoint deduplication, machine-only inventory, refresh errors and empty state');
})().catch(error => { console.error(error); process.exitCode = 1; });
