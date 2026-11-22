import {execFileSync} from 'node:child_process';
import {buildTool} from './build-tools.mjs';
import {mkdirSync,readFileSync,writeFileSync,existsSync,chmodSync,mkdtempSync,rmSync,readdirSync,copyFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const version='2.6.4';
// Pinned GitHub release asset digests; never trust a downloaded checksum alone.
const targets={
 'linux-x64':['linux-x86_64','61b659eaedba658fa66fe47d17e1426cdd77e5d02fa15fed447bb4357c09dfd6'],
 'linux-arm64':['linux-aarch64','f533ec25a7ea714e09f645615012200278058525795cc3bb690ff011aec1a70f'],
 'darwin-x64':['macos-x86_64','89fc28a6e6995259d76ce3f11775220e8a21c760e94df91a6a9db30a69b6982e'],
 'darwin-arm64':['macos-aarch64','4be1882d1aa36d31c1d6ba0596f2cf8a097e371f8da124212324b2e0f8df7e4b'],
 'win32-x64':['windows-x86_64','27af91e270e554709b048bd32327fefd2dfce5062ae1e8701af7550c6f525f84'],
 'win32-arm64':['windows-arm64','37023f8a3451c9234b17ee2089a03dc344ce90d803b5b359cb6c46682b0549b4'],
};
const target=targets[`${process.platform}-${process.arch}`];
if(!target)throw Error('Unsupported EasyTier native target');
const sha=b=>createHash('sha256').update(b).digest('hex');
const bin=path.join(root,'bin'),suffix=process.platform==='win32'?'.exe':'';
mkdirSync(bin,{recursive:true});
const names=['easytier-core','easytier-cli'];
if(!names.every(name=>{
 try{const m=JSON.parse(readFileSync(path.join(bin,name+'-manifest.json')));return m.version===version&&m.platform===process.platform&&m.arch===process.arch&&m.sha256===sha(readFileSync(path.join(bin,name+suffix)));}catch{return false;}
})||!existsSync(path.join(bin,'easytier-LICENSE'))){
 const cache=path.join(root,'.cache');mkdirSync(cache,{recursive:true});
 const asset=`easytier-${target[0]}-v${version}.zip`,archive=path.join(cache,asset);
 if(!existsSync(archive)||sha(readFileSync(archive))!==target[1]){
  execFileSync(buildTool('curl'),['--fail','--location','--retry','2','--max-time','180','--output',archive,`https://github.com/EasyTier/EasyTier/releases/download/v${version}/${asset}`],{stdio:'inherit'});
 }
 if(sha(readFileSync(archive))!==target[1])throw Error('EasyTier release archive integrity check failed');
 const temp=mkdtempSync(path.join(os.tmpdir(),'newnoa-easytier-build-'));
 try{
  if(process.platform==='win32')execFileSync(buildTool('powershell'),['-NoProfile','-Command','Expand-Archive -LiteralPath $args[0] -DestinationPath $args[1]',archive,temp]);
  else execFileSync(buildTool('unzip'),['-q',archive,'-d',temp]);
  const files=[];const walk=dir=>{for(const e of readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,e.name);if(e.isDirectory())walk(p);else files.push(p);}};walk(temp);
  for(const name of names){
   const file=files.find(p=>path.basename(p)===name+suffix);if(!file)throw Error(`Missing ${name}`);
   const dest=path.join(bin,name+suffix);copyFileSync(file,dest);chmodSync(dest,0o755);
   writeFileSync(path.join(bin,name+'-manifest.json'),JSON.stringify({platform:process.platform,arch:process.arch,version,sha256:sha(readFileSync(dest)),archiveSha256:target[1]},null,2)+'\n');
  }
  const license=readFileSync(path.join(root,'licenses/easytier-LICENSE'));
  if(sha(license)!=='e3a994d82e644b03a792a930f574002658412f62407f5fee083f2555c5f23118')throw Error('EasyTier license integrity check failed');
  writeFileSync(path.join(bin,'easytier-LICENSE'),license);
 }finally{rmSync(temp,{recursive:true,force:true});}
}
