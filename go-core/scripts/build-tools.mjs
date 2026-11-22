import {accessSync,constants,realpathSync,statSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const goToolchain = 'go1.27.4';

export function trustedExecutable(candidate){
 if(!path.isAbsolute(candidate))throw new Error('Build executable path must be absolute');
 const resolved=realpathSync(candidate);
 if(!statSync(resolved).isFile())throw new Error('Build executable must be a file');
 accessSync(resolved,constants.X_OK);
 if(process.platform!=='win32'){
  for(let current=resolved;;current=path.dirname(current)){
   const info=statSync(current);
   if(![0,process.getuid()].includes(info.uid)||(info.mode&0o022)!==0)throw new Error('Build executable or parent is writable by another user');
   if(current===path.dirname(current))break;
  }
 }
 return resolved;
}

// Build tools use fixed installation locations, never the caller's mutable PATH.
export function buildTool(name){
 const windows=process.platform==='win32',system=process.env.SystemRoot||'C:\\Windows';
 const known={
  go:windows?[path.join(process.env.ProgramFiles||'C:\\Program Files','Go','bin','go.exe')]:[path.join(os.homedir(),'.local','go','bin','go'),'/usr/local/go/bin/go','/opt/homebrew/bin/go','/usr/local/bin/go','/usr/bin/go'],
  curl:windows?[path.join(system,'System32','curl.exe')]:['/usr/bin/curl','/bin/curl'],
  unzip:['/usr/bin/unzip','/bin/unzip'],
  powershell:[path.join(system,'System32','WindowsPowerShell','v1.0','powershell.exe')]
 };
 if(!Object.hasOwn(known,name))throw new Error('Unknown build executable');
 const configured=process.env['NEWNOA_'+name.toUpperCase()+'_BINARY'];
 if(configured)return trustedExecutable(configured);
 for(const candidate of known[name]){try{return trustedExecutable(candidate);}catch{}}
 throw new Error('Trusted '+name+' executable unavailable; set NEWNOA_'+name.toUpperCase()+'_BINARY to an absolute installed path');
}
