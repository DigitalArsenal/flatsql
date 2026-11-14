// FMB/FCK/FSG are valid SDS records but also FlatSQL's storage control frames.
// Keep generated examples outside engine feeds as ordinary size-prefixed SDS
// files. OPFS writable streams commit each bounded batch on close, so the demo
// never needs to load an entire raw collection into RAM to append to it.
export const RAW_TYPES = ['FMB','FCK','FSG'];
const stores=new Map();
async function directory(root,type,create=false) {
  let dir=await navigator.storage.getDirectory();
  for(const name of [root,'demo-raw',type])dir=await dir.getDirectoryHandle(name,{create});
  return dir;
}
async function collection(backend,root,type) {
  if(!RAW_TYPES.includes(type))throw new Error('Not a raw SDS collection');
  const key=`${backend}/${root}/${type}`;
  if(stores.has(key))return stores.get(key);
  const store={count:0,bytes:0,frames:[]};
  if(backend==='opfs') {
    try {
      const dir=await directory(root,type);
      for await(const [name,entry] of dir.entries()) {
        const match=/^(\d+)-[a-f0-9-]+\.fb$/.exec(name);
        if(entry.kind!=='file'||!match)continue;
        const file=await entry.getFile();
        // An interrupted writable stream leaves its new entry empty. Only
        // successfully closed batches contribute to the persisted count.
        if(file.size){store.count+=Number(match[1]);store.bytes+=file.size;}
      }
    }catch(error){if(error.name!=='NotFoundError')throw error;}
  }
  stores.set(key,store);return store;
}
export async function rawStore({action,backend,root,type,frames,count}) {
  if(action==='rawReset') {
    for(const key of stores.keys())if(key.startsWith(`${backend}/${root}/`))stores.delete(key);
    return {};
  }
  const store=await collection(backend,root,type);
  if(action==='rawAppend') {
    // Validate framing before publishing a batch or trusting its count.
    let at=0,actual=0;
    const view=new DataView(frames.buffer,frames.byteOffset,frames.byteLength);
    while(at<frames.length) {
      if(at+12>frames.length)throw new Error('Incomplete SDS frame');
      const size=view.getUint32(at,true);
      if(size<8||at+4+size>frames.length)throw new Error('Invalid SDS frame length');
      const identifier=String.fromCharCode(...frames.subarray(at+8,at+12));
      if(identifier!==`$${type}`)throw new Error('SDS file identifier mismatch');
      at+=size+4;actual++;
    }
    if(actual!==count||!actual)throw new Error('SDS batch count mismatch');
    if(backend==='opfs') {
      const dir=await directory(root,type,true),name=`${count}-${crypto.randomUUID()}.fb`;
      let writer;
      try {
        writer=await(await dir.getFileHandle(name,{create:true})).createWritable();
        await writer.write(frames);await writer.close();
      }catch(error){await writer?.abort().catch(()=>{});await dir.removeEntry(name).catch(()=>{});throw error;}
    }else store.frames.push(frames);
    store.count+=count;store.bytes+=frames.length;
  }
  return {count:store.count,bytes:store.bytes};
}
