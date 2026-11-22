// Compile and inspect native components without replacing the current desktop bin/.
import {execFileSync} from 'node:child_process';
import {buildTool,goToolchain} from './build-tools.mjs';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {nativeTarget} from './native-artifact.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.join(root,'.local','cross-build-result.json');
await mkdir(path.dirname(output),{recursive:true});
const result={passed:false,status:'running',scope:'cross compilation and executable header inspection only; no target execution, Electron bundle, signing or installer',
 goVersion:execFileSync(buildTool('go'),['version'],{encoding:'utf8',env:{...process.env,GOTOOLCHAIN:goToolchain}}).trim(),artifacts:[]};
const save=()=>writeFile(output,JSON.stringify(result,null,2)+'\n');
await save();
const temporary=await mkdtemp(path.join(tmpdir(),'newnoa-cross-build-'));
try{
 for(const platform of ['linux','darwin','win32'])for(const arch of ['x64','arm64']){
  const env={...process.env,GOTOOLCHAIN:goToolchain,GOOS:{linux:'linux',darwin:'darwin',win32:'windows'}[platform],
   GOARCH:arch==='x64'?'amd64':'arm64',CGO_ENABLED:'0'};
  for(const component of ['core','frpc']){
   const binary=path.join(temporary,component+(platform==='win32'?'.exe':''));
   const cwd=component==='core'?root:path.resolve(root,'third_party/frp');
   const args=component==='core'?['build','-trimpath','-o',binary,'.']:['build','-tags','noweb','-trimpath','-o',binary,'./cmd/frpc'];
   execFileSync(buildTool('go'),args,{cwd,env,stdio:'inherit'});
   const body=await readFile(binary),target=nativeTarget(body);
   if(target.platform!==platform||target.arch!==arch)throw new Error('Cross-built executable has an unexpected target');
   result.artifacts.push({component,platform,arch,bytes:body.length,sha256:createHash('sha256').update(body).digest('hex')});
   await rm(binary);
   await save();
   console.log(`Verified ${component}: ${platform}/${arch}`);
  }
 }
 result.passed=true;result.status='complete';
}catch(error){
 result.status='failed';
 throw error;
}finally{
 await save();
 await rm(temporary,{recursive:true,force:true});
}
