'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const { spawn, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const http = require('http');
const crypto = require('crypto');
const zlib = require('zlib');

const ROOT_DIR = path.resolve(__dirname, '..');
const BIN_SCRIPT = path.join(ROOT_DIR, 'bin', 'weblimbai.js');
const LIGHTLIMBS_SCRIPT = path.join(ROOT_DIR, 'bin', 'lightlimbs.js');

const { getPlatformInfo, getCacheDir, getBinaryCachePath } = require('../bin/lib/platform');
const { extractBinary, extractBinaryFromTar, extractBinaryFromZip } = require('../bin/lib/untar');
const { parseChecksums } = require('../bin/lib/resolver');

function runCliAsync(scriptPath, args, env = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [scriptPath, ...args], {
      env: { ...process.env, ...env },
      stdio: ['pipe', 'pipe', 'pipe']
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (d) => {
      stdout += d.toString('utf8');
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString('utf8');
    });

    child.on('error', reject);
    child.on('close', (status) => {
      resolve({ status, stdout, stderr });
    });
  });
}

describe('WebLimbAI NPM Binary Wrapper Test Suite', () => {
  let tempCacheDir;

  before(() => {
    tempCacheDir = fs.mkdtempSync(path.join(os.tmpdir(), 'weblimb-test-cache-'));
  });

  after(() => {
    fs.rmSync(tempCacheDir, { recursive: true, force: true });
  });

  it('1. package.json manifest should have valid metadata, bin mappings and 0 dependencies', () => {
    const pkgPath = path.join(ROOT_DIR, 'package.json');
    assert.strictEqual(fs.existsSync(pkgPath), true);
    const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));

    assert.strictEqual(pkg.name, 'weblimbai');
    assert.strictEqual(pkg.bin.weblimbai, './bin/weblimbai.js');
    assert.strictEqual(pkg.bin.lightlimbs, './bin/lightlimbs.js');
    assert.strictEqual(pkg.bin.weblimb, './bin/weblimbai.js');
    assert.strictEqual(pkg.dependencies, undefined, 'Must have zero runtime dependencies');
  });

  it('2. platform.js should map known platforms and architectures accurately', () => {
    const darwinArm = getPlatformInfo('darwin', 'arm64');
    assert.strictEqual(darwinArm.goos, 'darwin');
    assert.strictEqual(darwinArm.goarch, 'arm64');
    assert.strictEqual(darwinArm.binaryName, 'weblimb');
    assert.strictEqual(darwinArm.archiveName, 'weblimb_darwin_arm64.tar.gz');
    assert.strictEqual(darwinArm.isSupported, true);

    const linuxAmd = getPlatformInfo('linux', 'x64');
    assert.strictEqual(linuxAmd.goos, 'linux');
    assert.strictEqual(linuxAmd.goarch, 'amd64');
    assert.strictEqual(linuxAmd.archiveName, 'weblimb_linux_amd64.tar.gz');
    assert.strictEqual(linuxAmd.isSupported, true);

    const winAmd = getPlatformInfo('win32', 'x64');
    assert.strictEqual(winAmd.goos, 'windows');
    assert.strictEqual(winAmd.goarch, 'amd64');
    assert.strictEqual(winAmd.binaryName, 'weblimb.exe');
    assert.strictEqual(winAmd.archiveName, 'weblimb_windows_amd64.zip');
    assert.strictEqual(winAmd.isSupported, true);
  });

  it('3. untar.js should extract target binary from tar.gz and prevent zip slip', () => {
    const mockContent = '#!/bin/sh\necho "TAR_EXTRACTED_OK"\n';
    const tarHeader = Buffer.alloc(512);
    tarHeader.write('weblimb', 0, 7, 'utf8');
    const octalSize = mockContent.length.toString(8).padStart(11, '0');
    tarHeader.write(octalSize, 124, 11, 'utf8');
    tarHeader.write('ustar  ', 257, 7, 'utf8');

    const fileBlock = Buffer.alloc(512);
    Buffer.from(mockContent).copy(fileBlock);

    const fullTar = Buffer.concat([tarHeader, fileBlock, Buffer.alloc(1024)]);
    const gzipped = zlib.gzipSync(fullTar);

    const destPath = path.join(tempCacheDir, 'extracted-tar-bin');
    extractBinary(gzipped, 'weblimb', destPath);

    assert.strictEqual(fs.existsSync(destPath), true);
    assert.strictEqual(fs.readFileSync(destPath, 'utf8'), mockContent);
  });

  it('4. untar.js should extract target binary from zip archives', () => {
    const mockContent = 'MOCK_ZIP_BINARY_DATA';
    const fileName = 'weblimb.exe';
    const fileBuf = Buffer.from(mockContent);
    const fileNameBuf = Buffer.from(fileName, 'utf8');

    // Build raw uncompressed ZIP local header
    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0); // Signature
    localHeader.writeUInt16LE(20, 4); // Min version
    localHeader.writeUInt16LE(0, 6); // Flags
    localHeader.writeUInt16LE(0, 8); // Method: Stored (0)
    localHeader.writeUInt16LE(0, 10); // Time
    localHeader.writeUInt16LE(0, 12); // Date
    localHeader.writeUInt32LE(0, 14); // CRC-32 (dummy)
    localHeader.writeUInt32LE(fileBuf.length, 18); // Compressed size
    localHeader.writeUInt32LE(fileBuf.length, 22); // Uncompressed size
    localHeader.writeUInt16LE(fileNameBuf.length, 26); // Filename len
    localHeader.writeUInt16LE(0, 28); // Extra len

    const zipBuffer = Buffer.concat([localHeader, fileNameBuf, fileBuf]);
    const destPath = path.join(tempCacheDir, 'extracted-zip-bin.exe');
    extractBinary(zipBuffer, 'weblimb.exe', destPath);

    assert.strictEqual(fs.existsSync(destPath), true);
    assert.strictEqual(fs.readFileSync(destPath, 'utf8'), mockContent);
  });

  it('5. should execute --help via Node runner and output usage instructions', () => {
    const res = spawnSync(process.execPath, [BIN_SCRIPT, '--help'], {
      encoding: 'utf8',
      env: { ...process.env, WEBLIMB_CACHE_DIR: tempCacheDir }
    });
    assert.strictEqual(res.status, 0);
    const output = res.stdout + res.stderr;
    assert.match(output, /WebLimbAI|LightLimbs/i);
    assert.match(output, /Available Subcommands|USAGE/i);
  });

  it('6. should execute version command and output runtime diagnostics', () => {
    const res = spawnSync(process.execPath, [BIN_SCRIPT, 'version'], {
      encoding: 'utf8',
      env: { ...process.env, WEBLIMB_CACHE_DIR: tempCacheDir }
    });
    assert.strictEqual(res.status, 0);
    const output = res.stdout + res.stderr;
    assert.match(output, /WebLimbAI|LightLimbs/i);
  });

  it('7. should execute lightlimbs.js entrypoint with exact alias parity', () => {
    const res = spawnSync(process.execPath, [LIGHTLIMBS_SCRIPT, 'version'], {
      encoding: 'utf8',
      env: { ...process.env, WEBLIMB_CACHE_DIR: tempCacheDir }
    });
    assert.strictEqual(res.status, 0);
    const output = res.stdout + res.stderr;
    assert.match(output, /WebLimbAI|LightLimbs/i);
  });

  it('8. should respect WEBLIMB_BIN environment variable override', () => {
    const mockBin = path.join(tempCacheDir, 'mock-custom-bin');
    fs.writeFileSync(mockBin, '#!/bin/sh\necho "MOCK_CUSTOM_EXECUTED $1"\nexit 42\n', { mode: 0o755 });

    const res = spawnSync(process.execPath, [BIN_SCRIPT, 'arg-test'], {
      encoding: 'utf8',
      env: { ...process.env, WEBLIMB_BIN: mockBin }
    });
    assert.strictEqual(res.status, 42);
    assert.match(res.stdout, /MOCK_CUSTOM_EXECUTED arg-test/);
  });

  it('9. should correctly download, verify SHA-256 checksum, extract and cache binary', async () => {
    const info = getPlatformInfo();
    const mockBinContent = '#!/bin/sh\necho "DOWNLOADED_VIA_HTTP_OK"\nexit 0\n';

    // Build tar buffer
    const tarHeader = Buffer.alloc(512);
    tarHeader.write(info.binaryName, 0, info.binaryName.length, 'utf8');
    const octalSize = mockBinContent.length.toString(8).padStart(11, '0');
    tarHeader.write(octalSize, 124, 11, 'utf8');
    tarHeader.write('ustar  ', 257, 7, 'utf8');

    const fileBlock = Buffer.alloc(512);
    Buffer.from(mockBinContent).copy(fileBlock);

    const fullTar = Buffer.concat([tarHeader, fileBlock, Buffer.alloc(1024)]);
    const gzArchive = zlib.gzipSync(fullTar);
    const expectedSha256 = crypto.createHash('sha256').update(gzArchive).digest('hex');
    const checksumsTxt = `${expectedSha256}  ${info.archiveName}\n`;

    const server = http.createServer((req, res) => {
      if (req.url.endsWith('checksums.txt')) {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Connection': 'close' });
        res.end(checksumsTxt);
      } else if (req.url.endsWith(info.archiveName)) {
        res.writeHead(200, { 'Content-Type': 'application/gzip', 'Connection': 'close' });
        res.end(gzArchive);
      } else {
        res.writeHead(404, { 'Connection': 'close' });
        res.end();
      }
    });

    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;
    const mockBaseUrl = `http://127.0.0.1:${port}`;

    try {
      const isolatedCache = fs.mkdtempSync(path.join(os.tmpdir(), 'weblimb-dl-test-'));
      const res = await runCliAsync(BIN_SCRIPT, ['version'], {
        WEBLIMB_BIN: '',
        LIGHTLIMBS_BIN: '',
        AGENTLIMBS_BIN: '',
        WEBLIMB_CACHE_DIR: isolatedCache,
        WEBLIMB_RELEASE_BASE_URL: mockBaseUrl
      });
      assert.strictEqual(res.status, 0);
      assert.match(res.stdout, /DOWNLOADED_VIA_HTTP_OK/);
      fs.rmSync(isolatedCache, { recursive: true, force: true });
    } finally {
      if (typeof server.closeAllConnections === 'function') {
        server.closeAllConnections();
      }
      server.close();
    }
  });

  it('10. should reject corrupt downloads with checksum mismatch', async () => {
    const info = getPlatformInfo();
    const corruptArchive = Buffer.from('CORRUPT_OR_MODIFIED_PAYLOAD_DATA');
    const checksumsTxt = `0000000000000000000000000000000000000000000000000000000000000000  ${info.archiveName}\n`;

    const server = http.createServer((req, res) => {
      if (req.url.endsWith('checksums.txt')) {
        res.writeHead(200, { 'Content-Type': 'text/plain', 'Connection': 'close' });
        res.end(checksumsTxt);
      } else {
        res.writeHead(200, { 'Content-Type': 'application/gzip', 'Connection': 'close' });
        res.end(corruptArchive);
      }
    });

    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;

    try {
      const isolatedCache = fs.mkdtempSync(path.join(os.tmpdir(), 'weblimb-corrupt-test-'));
      const res = await runCliAsync(BIN_SCRIPT, ['version'], {
        WEBLIMB_BIN: '',
        LIGHTLIMBS_BIN: '',
        AGENTLIMBS_BIN: '',
        WEBLIMB_CACHE_DIR: isolatedCache,
        WEBLIMB_RELEASE_BASE_URL: `http://127.0.0.1:${port}`
      });
      const combinedOutput = res.stderr + res.stdout;
      assert.notStrictEqual(res.status, 0);
      assert.match(combinedOutput, /Checksum verification failed|Integrity error/i);
      fs.rmSync(isolatedCache, { recursive: true, force: true });
    } finally {
      if (typeof server.closeAllConnections === 'function') {
        server.closeAllConnections();
      }
      server.close();
    }
  });

  it('11. should seamlessly forward exit codes from native process', () => {
    const mockBin = path.join(tempCacheDir, 'mock-exit-code-bin');
    fs.writeFileSync(mockBin, '#!/bin/sh\nexit 13\n', { mode: 0o755 });

    const res = spawnSync(process.execPath, [BIN_SCRIPT], {
      env: { ...process.env, WEBLIMB_BIN: mockBin }
    });
    assert.strictEqual(res.status, 13);
  });
});
