// Run: node tests/unit_run_row.js (no browser or network required).
//
// 动作页选「全部仓库」时，三个仓库的运行是混在一张列表里的。卡片上不写仓库名，
// 遇到重名的工作流（example-org/jit-lab-a 和 jit-lab-b 都有「JIT Parallel Probe」）
// 就分不清谁说谁。这里钉住两件事：
//   1. 混排时卡片带仓库名，展开详情里也有一行「仓库」
//   2. 已经按具体仓库筛过之后不再重复显示（那时候写了也是噪音）
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new Function(script);                      // 整段先过语法

const i18nStart = script.indexOf('const I18N_EN = {');
const i18nSrc = script.slice(i18nStart, script.indexOf('\n};', i18nStart) + 3);
// statusOf → fillOpenLogs 这一段里有 statusOf、fmtDur、relTime、runRowHtml、jobsBox
const code = script.slice(script.indexOf('function statusOf('), script.indexOf('function fillOpenLogs('));

const context = vm.createContext({
  esc: s => String(s).replaceAll('&', '&amp;').replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
  LANG: 'zh',
  actRepo: '',                             // 当前筛选的仓库（空 = 全部仓库）
  GH: {logged_in: false},                  // 未登录 gh 时 job 区退化成一句提示
  fmtFull: ms => (ms ? '2026-11-12 05:00' : '—'),
});
vm.runInContext(i18nSrc, context);
vm.runInContext(`
  function t(zh) { return LANG === 'en' ? (I18N_EN[zh] || zh) : zh; }
  function L(zh, en) { return LANG === 'en' ? en : zh; }
`, context);
vm.runInContext(code, context);

const run = {
  id: 12345678901, repo: 'example-org/relay-test', name: 'JIT Parallel Probe',
  run_number: 6, run_attempt: 1, status: 'completed', conclusion: 'success',
  title: '探测', sha: '4a0953f', author: 'example-org', actor: 'example-org',
  trigger: 'workflow_dispatch', branch: 'main', path: '.github/workflows/probe.yml',
  url: 'https://github.com/x/y/actions/runs/1', started_at: 1762900000000,
  commit_at: 1762898400000,      // 提交比运行早 26 分钟（手动触发就是这样）
  updated_at: 1762900060000, duration_s: 60, seen_at: 1762900000000,
};

// 1) 全部仓库：卡片上要能看见是哪个仓库
let out = context.runRowHtml(run);
assert.match(out, /class="repo"/, '混排时卡片带仓库名');
assert.match(out, /example-org\/relay-test/);
assert.match(out, /JIT Parallel Probe/);
assert.match(out, /4a0953f/);
// 窄面板里标题会被截成「Electro…」，鼠标悬停得能看到全文
assert.match(out, /<span class="t" title="探测">探测<\/span>/, '标题带悬浮全文');

// 6.5) 提交时间紧跟在 sha 后面——它和右边的「9 小时前」（运行开始）不是一回事，
//      手动触发时两者能差半小时以上，得能分得出来
assert.match(out, /class="ct"/, '卡片上有提交时间');
const subHtml = out.match(/<div class="run-sub">([\s\S]*?)<\/div>/)[1];
assert.ok(subHtml.indexOf('class="sha"') < subHtml.indexOf('class="ct"'),
  '提交时间在 sha 后面');
assert.match(subHtml, /class="ct" title="提交于 [^"]*">[^<]+<\/span>/, '时间带悬浮的完整时间');
assert.match(subHtml, /\d{1,2}-\d{1,2} \d{2}:\d{2}/, '时间按「月-日 时:分」显示');
// 详情里也要写清楚这是提交时间
assert.match(out, /<span class="hint"> · 提交于 /, '详情里注明是提交时间');

// 6.6) 没有提交时间（API 拉回来的行）就不显示，不留下空占位或多余的「·」
let noCommit = context.runRowHtml({...run, commit_at: 0});
assert.doesNotMatch(noCommit, /class="ct"/, '缺提交时间就不显示');
assert.doesNotMatch(noCommit, /提交于 /, '详情里也不提');

// 2) 仓库名在标题行下面那一行（run-sub），不能挤到右上角的标签区去——
//    窄屏时 run-side 是 hide 的，放那儿等于又看不见了
const sub = out.match(/<div class="run-sub">([\s\S]*?)<\/div>/)[1];
assert.match(sub, /class="repo"/, '仓库名在 run-sub 这一行里');
const side = out.match(/<div class="run-side">([\s\S]*?)<\/div>/)[1];
assert.doesNotMatch(side, /relay-test/, '别把仓库名塞进窄屏会隐藏的右上角');

// 3) 展开详情里也有一行「仓库」
const detail = out.match(/<div class="run-detail"[\s\S]*<\/div>\s*<\/div>$/)[0];
assert.match(detail, /仓库/, '详情里有仓库这一行');
assert.match(detail, /\.github\/workflows\/probe\.yml/, '工作流文件那行还在');

// 4) 已经按仓库筛选过：不再重复显示
context.actRepo = 'example-org/relay-test';
out = context.runRowHtml(run);
assert.doesNotMatch(out, /class="repo"/, '筛过之后不重复显示仓库');
assert.match(out, /<div class="kv"><span class="k">仓库<\/span>/, '详情里仍然给一行，确认自己看的是哪条');

// 5) 英文界面：标签跟着换
context.LANG = 'en';
out = context.runRowHtml(run);
assert.match(out, /<span class="k">repo<\/span>/, '英文环境下详情标签是 repo');
context.LANG = 'zh';
context.actRepo = '';

// 6) 没有仓库字段（比如 API 拉回来的行）也不能炸
out = context.runRowHtml({...run, repo: ''});
assert.doesNotMatch(out, /class="repo"/);
assert.match(out, /—/, '缺字段时详情给个破折号，不要空着或报错');

console.log('PASS: 卡片显示所属仓库，按仓库筛选后不重复，详情里始终有一行');
