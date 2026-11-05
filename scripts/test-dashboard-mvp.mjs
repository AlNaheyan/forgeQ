// Browser check against an isolated, already-running ForgeQ test stack.
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const base=process.env.FORGEQ_MVP_URL;
const key=process.env.FORGEQ_MVP_KEY;
const chromePath=process.env.CHROME_PATH;
if(!base || !key || !chromePath) throw new Error('Local test URL, key and Chrome path are required');
const profile=await mkdtemp(join(tmpdir(),'forgeq-mvp-browser-'));
const chrome=spawn(chromePath,['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--remote-debugging-port=0',`--user-data-dir=${profile}`,'about:blank'],{stdio:'ignore'});
chrome.on('error',()=>{});
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let socket;
try {
 let port;
 for(let i=0;i<100;i++){try{port=(await readFile(join(profile,'DevToolsActivePort'),'utf8')).split('\n')[0];break}catch{await pause(100)}}
 if(!port)throw new Error('Isolated Chrome did not start');
 const tabs=await(await fetch(`http://127.0.0.1:${port}/json`)).json();
 socket=new WebSocket(tabs.find(t=>t.type==='page').webSocketDebuggerUrl);
 await new Promise((resolve,reject)=>{socket.onopen=resolve;socket.onerror=reject});
 let seq=0;const pending=new Map();
 socket.onmessage=event=>{const m=JSON.parse(event.data);if(m.id){const p=pending.get(m.id);pending.delete(m.id);m.error?p.reject(new Error('Browser protocol error')):p.resolve(m.result)}};
 const call=(method,params={})=>new Promise((resolve,reject)=>{const id=++seq;pending.set(id,{resolve,reject});socket.send(JSON.stringify({id,method,params}))});
 const evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw new Error('Dashboard script failed');return r.result.value};
 const until=async expression=>{for(let i=0;i<150;i++){if(await evaluate(expression))return;await pause(100)}throw new Error('Dashboard condition timed out')};
 await call('Page.navigate',{url:base+'/dashboard'});
 await until(`!!document.getElementById('api-key')`);
 await evaluate(`document.getElementById('api-key').value=${JSON.stringify(key)}; document.getElementById('auth-form').requestSubmit(); true`);
 await until(`document.getElementById('health-text').textContent==='LIVE' && state.jobTypes.some(t=>t.name==='invoice.create')`);
 await evaluate(`document.getElementById('job-type').value='invoice.create'; document.getElementById('job-type').dispatchEvent(new Event('change')); document.getElementById('payload-json').value='[]'; document.getElementById('launch-form').requestSubmit(); true`);
 await until(`document.getElementById('toast').textContent.includes('Payload must be a JSON object')`);
 await evaluate(`document.getElementById('payload-json').value=JSON.stringify({invoice_id:'inv-dashboard',amount_cents:1500}); document.getElementById('idempotency-key').value='dashboard-invoice'; document.getElementById('launch-form').requestSubmit(); true`);
 await until(`state.jobs.some(j=>j.type==='invoice.create' && j.payload?.invoice_id==='inv-dashboard' && j.status==='succeeded' && j.result?.amount_cents===1500)`);
 await until(`document.getElementById('jobs').textContent.includes('inv-dashboard') && document.getElementById('jobs').textContent.includes('1500')`);
 await evaluate(`document.getElementById('payload-json').value=JSON.stringify({invoice_id:'inv-invalid',amount_cents:0}); document.getElementById('launch-form').requestSubmit(); true`);
 await until(`state.jobs.some(j=>j.type==='invoice.create' && j.payload?.invoice_id==='inv-invalid' && j.status==='dead_letter' && j.last_error==='invalid_payload')`);
 if(await evaluate(`document.getElementById('api-key').value!=='' || document.body.innerText.includes(${JSON.stringify(key)})`))throw new Error('Credential visible in dashboard');
 console.log('Dashboard MVP passed: custom job, result, invalid payload and tenant access');
} finally {
 socket?.close();chrome.kill();await new Promise(resolve=>chrome.exitCode!==null?resolve():chrome.once('exit',resolve));
 await rm(profile,{recursive:true,force:true});
}
