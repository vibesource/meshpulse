const assert=require('node:assert/strict');require('../frontend/message_bridge.js');
(async()=>{let id=0;const sent=[];const b=new MessageBridge(x=>sent.push(x),()=>String(++id));let last;b.subscribe(e=>last=e);
b.requestHistory();b.receive({type:'new_message',message:{channel_index:1,text:'live'}});b.receive({type:'messages_data',messages:{1:[]},channel_names:{1:'TAK'}});assert.equal(b.history[1][0].text,'live');assert.equal(last.type,'history');
const a=b.sendMessage('a',null,1),c=b.sendMessage('c',null,2);b.receive({type:'send_result',request_id:'2',success:true});b.receive({type:'send_result',request_id:'1',success:false});assert.equal((await a).success,false);assert.equal((await c).success,true);
const d=b.sendMessage('d',null,1);b.cancelRequests(['3']);await assert.rejects(d,/cancelled/);
const e=b.sendMessage('e',null,1);b.disconnected();await assert.rejects(e,/outcome unknown/);console.log('Bridge: history race, reverse responses, cancellation, disconnect passed');})().catch(e=>{console.error(e);process.exit(1)});
