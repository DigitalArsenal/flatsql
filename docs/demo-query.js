import { ByteBuffer } from 'flatbuffers';
import { QRP } from 'spacedatastandards.org/lib/js/QRP/QRP.js';
import { qrpStatus } from 'spacedatastandards.org/lib/js/QRP/qrpStatus.js';

const PREVIEW_ROWS=100;
const yieldToBrowser=()=>new Promise(resolve=>setTimeout(resolve,0));

// Transport chunks can split a length prefix or a QRP page. Keep only the
// unfinished frame, rather than collecting the entire query result in RAM.
async function* resultPages(chunks,onBytes) {
 let pending=new Uint8Array();
 for await(const chunk of chunks) {
  onBytes(chunk.length);
  const data=new Uint8Array(pending.length+chunk.length);
  data.set(pending);data.set(chunk,pending.length);
  let at=0;
  while(at+4<=data.length) {
   const size=new DataView(data.buffer,at,4).getUint32(0,true);
   if(size<8)throw new Error('Invalid query result frame');
   if(at+4+size>data.length)break;
   yield QRP.getSizePrefixedRootAsQRP(new ByteBuffer(data.subarray(at,at+4+size)));
   at+=4+size;
  }
  pending=data.slice(at);
 }
 if(pending.length)throw new Error('Incomplete query result stream');
}

function cellText(cell) {
 switch(cell.KIND()) {
  case 0:return 'NULL';
  case 1:return String(cell.BOOL_VALUE());
  case 2:return String(cell.INT_VALUE());
  case 3:return String(cell.FLOAT_VALUE());
  case 4:{const text=cell.TEXT_VALUE()||'';return text.length>2000?text.slice(0,2000)+'…':text;}
  default:return `${cell.bytesValueLength()} bytes`;
 }
}

export function createQueryView({results,summary,state,elapsed,cancel}) {
 let controller=null;
 cancel.onclick=()=>{
  if(controller){controller.abort();state.textContent='Cancelling query…';cancel.disabled=true;}
 };
 return {
  get running(){return controller!==null;},
  reset(){state.textContent='No SQL query';state.dataset.state='idle';elapsed.textContent='0.000 s';summary.textContent='';cancel.disabled=true;},
  async run(db,request) {
   controller=new AbortController();
   const signal=controller.signal,started=performance.now();
   let total=0,shown=0,totalBytes=0,columns=false,outcome='complete',failure=null,lastYield=started;
   const table=document.createElement('table'),head=table.createTHead(),body=table.createTBody();
   results.replaceChildren(table);
   results.setAttribute('aria-busy','true');
   state.dataset.state='running';state.textContent='Query running…';cancel.disabled=false;
   const update=()=>{
    elapsed.textContent=`${((performance.now()-started)/1000).toFixed(3)} s`;
    summary.textContent=`${total.toLocaleString()} result row${total===1?'':'s'} received · showing ${shown.toLocaleString()}${total>shown?' (first 100 only)':''} · ${totalBytes.toLocaleString()} FlatBuffer bytes${outcome==='cancelled'||outcome==='error'?' · partial results':''}`;
   };
   update();const timer=setInterval(update,100);
   try {
    // SQL controls the row limit. Streaming removes the collector's 16 MiB
    // budget; the display remains bounded and Cancel uses the engine's signal.
    const chunks=db.query(request,{signal,timeoutMs:0,maxResultBytes:0});
    for await(const page of resultPages(chunks,n=>{totalBytes+=n;})) {
     if(page.STATUS()!==qrpStatus.Ok)throw new Error(page.MESSAGE()||`Query returned ${qrpStatus[page.STATUS()]}`);
     if(!columns&&page.columnsLength()) {
      const row=head.insertRow();
      for(let i=0;i<page.columnsLength();i++){const th=document.createElement('th');th.textContent=page.COLUMNS(i).NAME();row.append(th);}
      columns=true;
     }
     total+=page.rowsLength();
     for(let i=0;i<page.rowsLength()&&shown<PREVIEW_ROWS;i++,shown++) {
      const values=page.ROWS(i),row=body.insertRow();
      for(let j=0;j<values.cellsLength();j++)row.insertCell().textContent=cellText(values.CELLS(j));
     }
     // A perpetually full result ring otherwise chains microtasks, starving
     // painting, timer ticks and clicks even though the engine runs in a worker.
     if(performance.now()-lastYield>=16){update();await yieldToBrowser();lastYield=performance.now();}
    }
    if(signal.aborted)outcome='cancelled';
   }catch(error){
    if(signal.aborted)outcome='cancelled';
    else{outcome='error';failure=error;}
   }finally{
    clearInterval(timer);update();controller=null;cancel.disabled=true;
    results.setAttribute('aria-busy','false');state.dataset.state=outcome;
    state.textContent=outcome==='complete'?'Query complete':outcome==='cancelled'?'Query cancelled':`Query failed: ${failure.message}`;
   }
   if(failure)throw failure;
   return {cancelled:outcome==='cancelled',rows:total};
  }
 };
}
