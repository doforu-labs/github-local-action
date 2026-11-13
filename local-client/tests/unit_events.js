// Run: node tests/unit_events.js (no browser/network dependencies).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new Function(script);
class Element {
  constructor() { this.children = []; this.dataset = {}; this._html = ''; }
  set innerHTML(value) { this._html = value; this.children = []; }
  get innerHTML() { return this._html; }
  querySelector() { return null; }
  insertBefore(row, before) {
    row.parent = this;
    const index = before ? this.children.indexOf(before) : this.children.length;
    this.children.splice(index, 0, row);
  }
  get lastElementChild() { return this.children.at(-1); }
  remove() { this.parent.children.splice(this.parent.children.indexOf(this), 1); }
}
function setup(fetch) {
  const elements = Object.fromEntries(['events', 'eventsMini', 'listSub'].map(id => [id, new Element()]));
  const sources = [];
  const context = vm.createContext({
    document: {createElement: () => new Element()},
    $: id => elements[id], fetch, MAX_ROWS: 200, MAX_MINI: 5,
    latestId: 0, seen: new Set(), L: (zh,en) => zh, t: s => s,
    esc: s => String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),
    fmtFull: s => String(s), fmtTime: s => String(s), toneOf: () => 'accent', repoHtml: () => '',
    scheduleStats: () => {}, scheduleActions: () => {},
    EventSource: class {constructor(url) {this.url=url; sources.push(this);} addEventListener(name,cb) {this[name]=cb;}}
  });
  vm.runInContext(script.slice(script.indexOf('function emitRow('),script.indexOf('let statsTimer =')),context);
  vm.runInContext(script.slice(script.indexOf('async function startEvents('),script.indexOf('\nstartEvents();')),context);
  return {context,elements,sources};
}
const event = id => ({id,event:'push',at:id});
(async () => {
  let finish;
  const fixture = setup(url => {
    assert.equal(url, '/events?latest=1&limit=60');
    return new Promise(resolve => {finish=resolve;});
  });
  const {context:c, elements:el, sources} = fixture;
  const startup = c.startEvents();
  assert.equal(sources.length,0,'No SSE connection before history resolves');
  finish({ok:true,json:async()=>({events:[event(1296),event(1297)]})});
  await startup;
  assert.equal(sources[0].url,'/stream?after=1297');
  sources[0].onopen();
  assert.equal(el.listSub.textContent,'实时接收中');
  for (const id of [1298,1297,1295]) sources[0].gh({data:JSON.stringify(event(id))});
  assert.deepEqual(el.events.children.map(r=>Number(r.dataset.eventId)),[1298,1297,1296,1295]);
  assert.equal(c.latestId,1298,'Late events do not regress cursor');
  for (let id=1299;id<=1600;id++) c.renderEvent(event(id),true);
  assert.equal(el.events.children.length,200);
  assert.equal(el.eventsMini.children.length,5);
  assert.equal(Number(el.events.children[0].dataset.eventId),1600);
  assert.equal(Number(el.events.children.at(-1).dataset.eventId),1401);
  const details=c.eventDetailHtml({event:'workflow_job',repo:'owner/repo',payload:{workflow_job:{id:77,run_id:88,name:'<script>alert(1)</script>',workflow_name:'CI',runner_name:'Mac',conclusion:'success'}}});
  assert.match(details,/https:\/\/github.com\/owner\/repo\/actions\/runs\/88\/job\/77/);
  assert.match(details,/Runner · Mac/);
  assert.match(details,/success/);
  assert.doesNotMatch(details,/<script>/);
  assert.doesNotMatch(c.eventDetailHtml({event:'workflow_run',repo:'javascript:evil',payload:{workflow_run:{id:88,name:'CI'}}}),/href=/);
  for (const fetch of [async()=>{throw Error('offline');},async()=>({ok:false,status:500})]) {
    const failed=setup(fetch); await failed.context.startEvents();
    assert.equal(failed.sources[0].url,'/stream?after=0','Failed history falls back to full paginated replay');
  }
  const empty=setup(async()=>({ok:true,json:async()=>({events:[]})}));
  await empty.context.startEvents();
  assert.equal(empty.sources[0].url,'/stream?after=0');
  console.log('PASS: latest history, sequential startup, cursor, dedup/order/caps, run/job links, escaping, failure/empty recovery');
})().catch(err=>{console.error(err);process.exitCode=1;});
