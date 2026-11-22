import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdtemp, readFile, rename, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import { createGzip, createGunzip } from 'node:zlib';

const archiveHash = 'b25a15ae164f169d60316379b6197d444bf7f52b46c056ed4bd9e12d0cddc79a';
const binaryHash = '3857289b5a60d60c0df41a3347c93ee7ee849cc06913f686df31c981abfa8b0b';
const archiveName = 'pi-bolt-linux-x64-baseline';
const url = 'https://github.com/opensec-git/Pi-Bolt/releases/download/bolt-v1.2.1/' + archiveName + '.tar.xz';
const assets = fileURLToPath(new URL('../third_party/pi-bolt/', import.meta.url));
const target = path.join(assets, 'pi-linux-amd64.gz');

async function digest(file, compressed = false) {
  const hash = createHash('sha256');
  if (compressed) await pipeline(createReadStream(file), createGunzip(), hash);
  else await pipeline(createReadStream(file), hash);
  return hash.digest('hex');
}

export async function preparePiBolt() {
  try {
    if (await digest(target, true) === binaryHash) return;
  } catch (error) {
    if (error.code !== 'ENOENT' && error.code !== 'Z_DATA_ERROR' && error.code !== 'Z_BUF_ERROR') throw error;
  }
  const temp = await mkdtemp(path.join(tmpdir(), 'newnoa-pibolt-'));
  const stage = await mkdtemp(path.join(assets, '.stage-'));
  try {
    let archive = process.env.NEWNOA_PI_BOLT_ARCHIVE;
    if (!archive) {
      archive = path.join(temp, 'release.tar.xz');
      const response = await fetch(url, {signal: AbortSignal.timeout(300000)});
      if (!response.ok) throw new Error('Pi-Bolt download failed: HTTP ' + response.status);
      await pipeline(response.body, createWriteStream(archive, {mode: 0o600}));
    }
    if (await digest(archive) !== archiveHash) throw new Error('Pi-Bolt archive checksum mismatch');
    const files = ['pi', 'LICENSE', 'LICENSE.pi', 'THIRD_PARTY_NOTICES.md'];
    const result = spawnSync('/usr/bin/tar', ['-xf', archive, '-C', temp, ...files.map(f => archiveName + '/' + f)], {stdio:'inherit'});
    if (result.error || result.status !== 0) throw new Error('Could not extract verified Pi-Bolt release');
    const source = path.join(temp, archiveName);
    for (const notice of files.slice(1)) {
      if (!(await readFile(path.join(source,notice))).equals(await readFile(path.join(assets,notice)))) throw new Error('Pi-Bolt license mismatch');
    }
    const binary = path.join(source, 'pi');
    if (await digest(binary) !== binaryHash) throw new Error('Pi-Bolt executable checksum mismatch');
    const output = path.join(stage, 'pi.gz');
    await pipeline(createReadStream(binary), createGzip({level: 1}), createWriteStream(output));
    await rename(output, target);
  } finally {
    await rm(temp, {recursive:true, force:true});
    await rm(stage, {recursive:true, force:true});
  }
}
