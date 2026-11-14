const {chromium}=require('playwright'); const fs=require('fs'); const path=require('path'); const assert=require('assert/strict');
(async()=>{const browser=await chromium.launch({headless:true,args:['--no-sandbox']});const page=await browser.newPage();
await page.route('http://127.0.0.2:8090/test-auth', route=>route.fulfill({status:200,contentType:'text/html',body:'<html><head></head><body></body></html>'})); await page.goto('http://127.0.0.2:8090/test-auth');
await page.evaluate(()=>{document.body.innerHTML='';window.localStorage.clear();});
await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'../frontend/auth.js'),'utf8')});
await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'../frontend/message_bridge.js'),'utf8')});
const result=await page.evaluate(async()=>{
 class FakeSocket extends EventTarget { constructor(){super();this.readyState=1;this.sent=[]} send(s){this.sent.push(JSON.parse(s))} receive(d){this.dispatchEvent(new MessageEvent('message',{data:JSON.stringify(d)}))} }
 const socket=new FakeSocket(); MeshAuth.attach(socket);let n=0;
 const bridge=new MessageBridge(m=>socket.send(JSON.stringify(m)),()=>String(++n));
 socket.addEventListener('message',e=>bridge.receive(JSON.parse(e.data)));
 socket.addEventListener('meshauthcancel',e=>bridge.cancelRequests(e.detail));
 socket.receive({type:'auth_status',required:true,protected:['send_message']});
 const a=bridge.sendMessage('first',null,1).catch(e=>e.message);const b=bridge.sendMessage('second',null,2).catch(e=>e.message);
 const queuedBefore=socket.sent.length;document.querySelector('.mp-cancel').click();
 const cancel=[await a,await b];
 const c=bridge.sendMessage('third',null,2); const input=document.querySelector('.mp-auth input');input.value='test-password';document.querySelector('form.mp-auth').dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));
 socket.receive({type:'auth_result',ok:true});const request=socket.sent.find(x=>x.type==='send_message');socket.receive({type:'send_result',request_id:request.request_id,success:true});
 return {queuedBefore,cancel,request,result:await c}; });
assert.equal(result.queuedBefore,0);assert(result.cancel.every(x=>/cancelled/.test(x)));assert.equal(result.request.channel_index,2);assert.equal(result.result.success,true);console.log('Browser: auth cancellation, protected queue, login and correlated send passed');await browser.close();})().catch(e=>{console.error(e);process.exit(1)});
