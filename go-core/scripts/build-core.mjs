import {execFileSync} from 'node:child_process';
import {mkdirSync,readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {buildTool,goToolchain} from './build-tools.mjs';
const version=process.env.NEWNOA_RELEASE_VERSION??'0.4.1';
if(!/^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/.test(version))throw new Error('NEWNOA_RELEASE_VERSION must be a numeric SemVer version');
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');mkdirSync(path.join(root,'bin'),{recursive:true});
const goos={linux:'linux',darwin:'darwin',win32:'windows'}[process.platform];
const goarch={x64:'amd64',arm64:'arm64'}[process.arch];
if(!goos||!goarch)throw new Error('Unsupported native build target');
const environment={...process.env,GOTOOLCHAIN:goToolchain,GOOS:goos,GOARCH:goarch,CGO_ENABLED:'0'};
const target={platform:process.platform,arch:process.arch};
const binary=path.join(root,'bin',process.platform==='win32'?'newnoa-core.exe':'newnoa-core');
const noaTags=[];
if(process.env.NEWNOA_EMBED_PI_BOLT==='1'){
 if(goos!=='linux'||goarch!=='amd64')throw new Error('Pi-Bolt embedding currently supports Linux x64');
 const {preparePiBolt}=await import('./prepare-pi-bolt.mjs');
 await preparePiBolt();
 noaTags.push('-tags','noa_pibolt');
}
execFileSync(buildTool('go'),['build',...noaTags,'-trimpath','-ldflags',`-X main.buildVersion=${version}`,'-o',binary,'.'],{cwd:root,stdio:'inherit',env:environment});
writeFileSync(path.join(root,'bin','core-manifest.json'),JSON.stringify({...target,protocol:1,version,sha256:createHash('sha256').update(readFileSync(binary)).digest('hex')},null,2)+'\n');
// Test packages include the repository's pinned FRP source, built without its web UI.
const frpc=path.join(root,'bin',process.platform==='win32'?'frpc.exe':'frpc');
const source=path.resolve(root,'third_party/frp');
execFileSync(buildTool('go'),['build','-tags','noweb','-trimpath','-o',frpc,'./cmd/frpc'],{cwd:source,stdio:'inherit',env:environment});
writeFileSync(path.join(root,'bin','frpc-manifest.json'),JSON.stringify({...target,sha256:createHash('sha256').update(readFileSync(frpc)).digest('hex')},null,2)+'\n');

writeFileSync(path.join(root,'bin','frp-LICENSE'),readFileSync(path.join(source,'LICENSE')));
for(const name of ['tokenizer-v0.10.0-LICENSE','regexp2-v2.5.1-LICENSE'])writeFileSync(path.join(root,'bin',name),readFileSync(path.join(root,'third_party/network/licenses',name)));

await import('./build-easytier.mjs');
