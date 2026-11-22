import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';

export function nativeTarget(content){
 let platform,machine;
 if(content.length>=64&&content.subarray(0,4).equals(Buffer.from([0x7f,0x45,0x4c,0x46]))&&content[4]===2&&content[5]===1){
  platform='linux';machine=content.readUInt16LE(18);
  machine=({62:'x64',183:'arm64'})[machine];
 }else if(content.length>=32&&content.readUInt32LE(0)===0xfeedfacf){
  platform='darwin';machine=({[0x01000007]:'x64',[0x0100000c]:'arm64'})[content.readUInt32LE(4)];
 }else if(content.length>=64&&content.toString('ascii',0,2)==='MZ'){
  const offset=content.readUInt32LE(60);
  if(offset>=64&&offset<=content.length-26&&content.readUInt32LE(offset)===0x00004550&&content.readUInt16LE(offset+24)===0x20b){
   platform='win32';machine=({[0x8664]:'x64',[0xaa64]:'arm64'})[content.readUInt16LE(offset+4)];
  }
 }
 if(!platform||!machine)throw new Error('Unsupported native executable format or architecture');
 return {platform,arch:machine};
}

export async function verifyNativeArtifact(binary,manifestPath){
 const manifest=JSON.parse(await readFile(manifestPath,'utf8'));
 if(manifest.platform!==process.platform||manifest.arch!==process.arch)throw new Error('Native artifact target mismatch; rebuild for this platform');
 const content=await readFile(binary);
 const actual=createHash('sha256').update(content).digest('hex');
 if(!/^[a-f0-9]{64}$/.test(manifest.sha256)||manifest.sha256!==actual)throw new Error('Native artifact integrity check failed; rebuild');
 const target=nativeTarget(content);
 if(target.platform!==manifest.platform||target.arch!==manifest.arch)throw new Error('Native executable target differs from manifest; rebuild');
 return manifest;
}
