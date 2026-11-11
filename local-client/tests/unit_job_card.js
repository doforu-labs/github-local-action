// Run: node tests/unit_job_card.js (no browser or network required).
//
// job 卡片上「GitHub 状态未回写」这个小标，是唯一告诉用户「别去查日志了，
// 任务没问题，卡的是 GitHub」的地方。标不出来等于没有；标错了（正常 job 也标）
// 就是喊狼来了。这里把 jobCard 单独跑起来验两件事：
//   1. stale 的 job 才有标，正常 job 一个标都没有
//   2. 中英文都能出字，而且说的原因跟 stale 的类型对得上（机器空了 / runner 掉线）
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new Function(script);                       // 整段先过一遍语法

const i18nStart = script.indexOf('const I18N_EN = {');
const i18nEnd = script.indexOf('\n};', i18nStart) + 3;
const i18nSrc = script.slice(i18nStart, i18nEnd);
const code = script.slice(script.indexOf('function statusOf('), script.indexOf('function fillOpenLogs('));

const context = vm.createContext({
  // 只替掉真正碰 DOM / 随机数的两个依赖，其余按源码跑
  esc: s => String(s).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  LANG: 'zh',
});
vm.runInContext(i18nSrc, context);
vm.runInContext(`
  function t(zh) { return LANG === 'en' ? (I18N_EN[zh] || zh) : zh; }
  function L(zh, en) { return LANG === 'en' ? en : zh; }
`, context);
vm.runInContext(code, context);

const job = (extra) => Object.assign({
  id: 11, name: 'build', status: 'in_progress', conclusion: '',
  runner_name: 'lab-mac-1', started_at: '2026-11-11T21:00:00Z', completed_at: null,
  steps: [{number: 1, name: '检出代码', status: 'completed', conclusion: 'success',
           started_at: '2026-11-11T21:00:01Z', completed_at: '2026-11-11T21:00:04Z'}],
}, extra);

// 1) 正常在跑的 job：不带标
let out = context.jobCard(job({}));
assert.doesNotMatch(out, /job-stale/);
assert.match(out, /build/);
assert.match(out, /lab-mac-1/, 'runner 名字照旧显示');
assert.match(out, /data-joblog="11"/, '查看日志按钮还在');

// 2) 机器空了的那种：出中文标，并且原因写「空闲」
out = context.jobCard(job({stale: 'runner_idle'}));
assert.match(out, /job-stale/);
assert.match(out, /GitHub 状态未回写/);
assert.match(out, /已经空闲/, 'runner 空闲要说空闲');
assert.doesNotMatch(out, /已经离线/);

// 3) runner 掉线的那种：原因换成「离线」
out = context.jobCard(job({stale: 'runner_offline'}));
assert.match(out, /已经离线/);
assert.doesNotMatch(out, /已经空闲/);

// 4) 英文界面：文案得跟着换，不能留中文
context.LANG = 'en';
out = context.jobCard(job({stale: 'runner_idle'}));
assert.match(out, /GitHub hasn&#39;t caught up|GitHub hasn't caught up/);
assert.doesNotMatch(out, /状态未回写/);
assert.match(out, /already idle/);
out = context.jobCard(job({}));
assert.doesNotMatch(out, /job-stale/, '英文界面下正常 job 也不带标');
context.LANG = 'zh';

// 5) 标记本身不能把卡片撑坏：Label 的高度在 CSS 里收过
assert.match(html, /\.job-head \.job-stale \{[^}]*height:\s*18px/, 'stale 标有专门的尺寸');
assert.match(html, /"GitHub 状态未回写":/, 'i18n 词典里有这条');

console.log('PASS: stale 标只出现在该出现的 job 上，中英文与原因都对得上');
