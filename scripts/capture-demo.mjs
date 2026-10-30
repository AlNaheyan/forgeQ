// Real browser capture against an isolated running API. No mocked responses.
// Node >=22 and Chrome are required. Credentials arrive only through the env.
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, writeFile, mkdir, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const base=process.env.FORGEQUEUE_DEMO_URL;
const key=process.env.FORGEQUEUE_DEMO_KEY;
const output=process.env.FORGEQUEUE_DEMO_OUTPUT;
if (!base || !key || !output) throw new Error('Demo URL, key, and output are required');
const profile=await mkdtemp(join(tmpdir(),'forgequeue-browser-'));
const chrome=spawn(process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
 ['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--remote-debugging-port=0',`--user-data-dir=${profile}`,'about:blank'],{stdio:'ignore'});
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
 const until=async expression=>{for(let i=0;i<150;i++){if(await evaluate(expression))return;await pause(200)}throw new Error('Dashboard condition timed out')};
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
 await call('Page.navigate',{url:base+'/dashboard'});
 await until(`!!document.getElementById('api-key')`);
 await evaluate(`document.getElementById('api-key').value=${JSON.stringify(key)}; document.getElementById('auth-form').requestSubmit(); true`);
 await until(`document.getElementById('health-text').textContent==='LIVE' && state.workers.length===1`);
 const ids=[];
 for(let i=0;i<8;i++){
  const r=await fetch(base+'/v1/jobs',{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify({type:'sleep',payload:{duration_ms:1500},max_attempts:3})});
  if(r.status!==201)throw new Error(`Demo submission failed: ${r.status}`);
  ids.push((await r.json()).job_id);
 }
 await until(`state.jobs.some(j=>j.status==='running') && state.jobs.some(j=>j.status==='queued')`);
 await mkdir(output,{recursive:true});
 for(const stage of ['active','completed']){
  if(stage==='completed')await until(`state.jobs.length===8 && state.jobs.every(j=>j.status==='succeeded')`);
  if(await evaluate(`document.getElementById('api-key').value!=='' || document.body.innerText.includes(${JSON.stringify(key)})`))throw new Error('Credential visible in dashboard');
  const shot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});
  await writeFile(join(output,`dashboard-${stage}.png`),Buffer.from(shot.data,'base64'));
 }
 await writeFile(join(output,'demo.json'),JSON.stringify({jobs:ids.length,terminal:'succeeded',workerCount:1,workerConcurrency:2,payloadDurationMS:1500,browser:await call('Browser.getVersion'),capturedAt:new Date().toISOString()},null,2)+'\n');
 console.log('Captured real dashboard: 8 jobs succeeded, 1 worker, concurrency 2; credentials excluded.');
} finally {
 socket?.close();chrome.kill();await new Promise(resolve=>chrome.exitCode!==null?resolve():chrome.once('exit',resolve));
 await rm(profile,{recursive:true,force:true});
}
