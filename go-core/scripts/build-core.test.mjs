import assert from 'node:assert/strict';
import {test,mock} from 'node:test';
import {createHash} from 'node:crypto';

const commands=[],written=new Map();
mock.module('node:child_process',{namedExports:{execFileSync:(_file,args,options)=>{assert.equal(options.env.GOTOOLCHAIN,'go1.27.4');commands.push(args)}}});
mock.module('node:fs',{namedExports:{mkdirSync:()=>{},readFileSync:()=>Buffer.from('fixture'),writeFileSync:(file,value)=>written.set(file,value)}});
mock.module(new URL('./build-tools.mjs',import.meta.url).href,{namedExports:{buildTool:()=>'/fixture/go',goToolchain:'go1.27.4'}});
mock.module(new URL('./build-easytier.mjs',import.meta.url).href,{namedExports:{}});

let run=0;
test('release build stamps the binary and manifest with the same validated version',async()=>{
 const original=process.env.NEWNOA_RELEASE_VERSION;
 try {
  for(const version of [undefined,'0.1.127']){
   if(version===undefined)delete process.env.NEWNOA_RELEASE_VERSION;else process.env.NEWNOA_RELEASE_VERSION=version;
   commands.length=0;written.clear();
   await import(`./build-core.mjs?test=${run++}`);
   const expected=version??'0.1.0';
   assert.equal(commands[0][commands[0].indexOf('-ldflags')+1],`-X main.buildVersion=${expected}`);
   const manifest=JSON.parse([...written].find(([file])=>file.endsWith('core-manifest.json'))[1]);
   assert.equal(manifest.version,expected);assert.equal(manifest.protocol,1);
   assert.equal(manifest.sha256,createHash('sha256').update('fixture').digest('hex'));
  }
  for(const version of ['','v0.1.127','01.1.2','1.2','1.2.3-beta','1.2.3+build','1.2.3 -X forged','1.2.3\n']){
   process.env.NEWNOA_RELEASE_VERSION=version;commands.length=0;written.clear();
   await assert.rejects(import(`./build-core.mjs?test=${run++}`),/numeric SemVer/);
   assert.equal(commands.length,0);assert.equal(written.size,0);
  }
 } finally {if(original===undefined)delete process.env.NEWNOA_RELEASE_VERSION;else process.env.NEWNOA_RELEASE_VERSION=original;}
});
