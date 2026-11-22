import assert from 'node:assert/strict';
import {test} from 'node:test';
import {mkdtempSync,writeFileSync,chmodSync,rmSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {trustedExecutable,buildTool} from './build-tools.mjs';

test('build executables require absolute paths and reject writable files',()=>{
 const directory=mkdtempSync(path.join(os.homedir(),'.newnoa-build-tool-test-'));
 try {
  const executable=path.join(directory,'tool');writeFileSync(executable,'fixture',{mode:0o700});
  assert.equal(trustedExecutable(executable),executable);
  assert.throws(()=>trustedExecutable('go'));
  if(process.platform!=='win32'){chmodSync(executable,0o777);assert.throws(()=>trustedExecutable(executable));}
 } finally {rmSync(directory,{recursive:true,force:true});}
});

test('build tool resolution ignores PATH and validates explicit overrides',()=>{
 const directory=mkdtempSync(path.join(os.homedir(),'.newnoa-build-tool-test-'));const names=['NEWNOA_CURL_BINARY','PATH'];const old=Object.fromEntries(names.map(name=>[name,process.env[name]]));
 try {
  const executable=path.join(directory,'curl');writeFileSync(executable,'fixture',{mode:0o700});
  process.env.PATH=directory;delete process.env.NEWNOA_CURL_BINARY;
  assert.notEqual(buildTool('curl'),executable);assert.ok(path.isAbsolute(buildTool('go')));
  process.env.NEWNOA_CURL_BINARY=executable;assert.equal(buildTool('curl'),executable);
  process.env.NEWNOA_CURL_BINARY='curl';assert.throws(()=>buildTool('curl'),/absolute/);
  process.env.NEWNOA_CURL_BINARY=directory;assert.throws(()=>buildTool('curl'),/file/);
  process.env.NEWNOA_CURL_BINARY=path.join(directory,'missing');assert.throws(()=>buildTool('curl'));
  assert.throws(()=>buildTool('unrecognized'),/Unknown/);
  if(process.platform!=='win32'){chmodSync(directory,0o777);assert.throws(()=>trustedExecutable(executable),/parent is writable/);chmodSync(directory,0o700);}
 } finally {for(const name of names){if(old[name]===undefined)delete process.env[name];else process.env[name]=old[name];}rmSync(directory,{recursive:true,force:true});}
});
