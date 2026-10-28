// Generation stays off the UI thread; only one bounded batch is in flight.
import { GJN, GJNT, GJNFeatureT, GJNGeometryT, GJNGeometryType, GJNLinearRingT, GJNPositionT, GJNPropertyT } from 'spacedatastandards.org/lib/js/GJN/main.js';
import * as fb from 'flatbuffers';
import { rawStore } from './demo-raw-store.js';
const position=(x,y,z)=>new GJNPositionT(x,y,z??0,z!==undefined);
const ring=(a,b)=>new GJNLinearRingT([[a,a],[b,a],[b,b],[a,b],[a,a]].map(([x,y])=>position(x,y)));
const geometries=[
  new GJNGeometryT(GJNGeometryType.POLYGON,null,[],[ring(0,10),ring(4,6)]),
  new GJNGeometryT(GJNGeometryType.POINT,position(20,20,420)),
  new GJNGeometryT(GJNGeometryType.GEOMETRY_COLLECTION,null,[],[],[],[
    new GJNGeometryT(GJNGeometryType.POINT,position(-73.98,40.75)),
    new GJNGeometryT(GJNGeometryType.LINE_STRING,null,[position(0,0),position(1,0)]),
  ]),
];
let sequence=BigInt(Date.now());
function gjn(id,index) {
  const b=new fb.Builder(512);
  const feature=new GJNFeatureT(id,geometries[index%3],[new GJNPropertyT('source','synthetic demo')],0,false,true);
  GJN.finishSizePrefixedGJNBuffer(b,new GJNT([feature]).pack(b));
  return b.asUint8Array();
}
self.onmessage=async({data})=>{
  const {id,type,count,start,action}=data;
  try {
    if(action?.startsWith('raw')){self.postMessage({id,...await rawStore(data)});return;}
    const {generators}=await import('./sds/generators.mjs');
    const {generate}=await generators[type]();
    const parts=[];let bytes=0;
    // The byte bound is transport batching, never a total-record limit.
    for(let i=0;i<count && bytes<1024*1024;i++) {
      const key=crypto.randomUUID();
      const record=type==='GJN'?gjn(key,start+i):generate(key,++sequence);
      parts.push(record);bytes+=record.length;
    }
    const frames=new Uint8Array(bytes);let at=0;
    for(const part of parts){frames.set(part,at);at+=part.length;}
    self.postMessage({id,frames,count:parts.length},[frames.buffer]);
  } catch(error) {self.postMessage({id,error:error.message});}
};
