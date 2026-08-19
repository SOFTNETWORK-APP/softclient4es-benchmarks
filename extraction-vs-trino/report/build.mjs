#!/usr/bin/env node
/**
 * Renders the benchmark report to a print-ready PDF with headless Chrome (CDP).
 *
 *   node build.mjs [output.pdf]
 *
 * Two print passes, because Chrome's DevTools print API ignores `@page :first`
 * and draws its running header/footer inside the page margins:
 *
 *   1. cover.html  — zero margins  → full-bleed cover, unnumbered
 *   2. report.html — 17/16mm margins + running header/footer → body pages 1..N
 *
 * The two PDFs are then concatenated (cover first). Concatenation needs a Python
 * with `pypdf`; build.mjs looks for ./.venv/bin/python, then $PYTHON / python3.12 /
 * python3. Create the tool venv once with:
 *
 *   python3.12 -m venv report/.venv && report/.venv/bin/pip install pypdf
 */
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const COVER = path.join(HERE, 'cover.html');
const BODY = process.env.REPORT_HTML
  ? path.resolve(process.env.REPORT_HTML)
  : path.join(HERE, 'report.html');
const OUTPUT = path.resolve(process.argv[2] ??
  path.join(HERE, 'SoftClient4ES-vs-Trino-Extraction-Benchmark.pdf'));

const CHROME_CANDIDATES = [
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/Applications/Chromium.app/Contents/MacOS/Chromium',
  '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  '/usr/bin/google-chrome',
  '/usr/bin/chromium',
];

const MM = 1 / 25.4;                       // millimetres → inches
const A4 = { paperWidth: 210 * MM, paperHeight: 297 * MM };

const INK = '#64748b';
const ACCENT = '#1e40af';                  // --sc-blue

const headerTemplate = `
<div style="width:100%;font-family:-apple-system,'Helvetica Neue',sans-serif;font-size:7px;
            color:${INK};padding:0 18mm;-webkit-print-color-adjust:exact;">
  <div style="display:flex;justify-content:space-between;align-items:center;
              border-bottom:0.5px solid #e2e8f0;padding-bottom:3px;">
    <span style="letter-spacing:.06em;text-transform:uppercase;font-weight:600;
                 color:${ACCENT};">SoftClient4ES &nbsp;vs&nbsp; Trino</span>
    <span>Elasticsearch extraction benchmark</span>
  </div>
</div>`;

const footerTemplate = `
<div style="width:100%;font-family:-apple-system,'Helvetica Neue',sans-serif;font-size:7px;
            color:${INK};padding:0 18mm;-webkit-print-color-adjust:exact;">
  <div style="display:flex;justify-content:space-between;align-items:center;
              border-top:0.5px solid #e2e8f0;padding-top:4px;">
    <span>softclient4es.dev &nbsp;·&nbsp; Benchmark report &nbsp;·&nbsp; August 2026
          &nbsp;·&nbsp; measured on the released 0.2.5.1 build</span>
    <span><span class="pageNumber"></span> / <span class="totalPages"></span></span>
  </div>
</div>`;

// ── minimal CDP client ──────────────────────────────────────────────────────
class CDP {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    this.listeners = [];
    ws.addEventListener('message', (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.id !== undefined && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        msg.error ? reject(new Error(`${msg.error.message} ${JSON.stringify(msg.error.data ?? '')}`))
                  : resolve(msg.result);
      } else if (msg.method) {
        for (const l of this.listeners) l(msg);
      }
    });
  }
  static async connect(url) {
    const ws = new WebSocket(url);
    await new Promise((res, rej) => {
      ws.addEventListener('open', res, { once: true });
      ws.addEventListener('error', rej, { once: true });
    });
    return new CDP(ws);
  }
  send(method, params = {}, sessionId) {
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params, sessionId }));
    });
  }
  once(method) {
    return new Promise((resolve) => {
      const l = (msg) => {
        if (msg.method === method) {
          this.listeners = this.listeners.filter((x) => x !== l);
          resolve(msg.params);
        }
      };
      this.listeners.push(l);
    });
  }
  close() { this.ws.close(); }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Prints one HTML file and returns the PDF bytes. */
async function print(cdp, file, printOptions) {
  const { targetId } = await cdp.send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await cdp.send('Target.attachToTarget', { targetId, flatten: true });
  try {
    await cdp.send('Page.enable', {}, sessionId);
    await cdp.send('Runtime.enable', {}, sessionId);
    await cdp.send('Emulation.setEmulatedMedia', { media: 'print' }, sessionId);

    const loaded = cdp.once('Page.loadEventFired');
    await cdp.send('Page.navigate', { url: pathToFileURL(file).href }, sessionId);
    await loaded;

    // Inter must actually resolve, or the whole report silently falls back to
    // the system sans and stops looking like softclient4es.dev.
    const { result } = await cdp.send('Runtime.evaluate', {
      expression: `document.fonts.ready.then(() =>
        [...document.fonts].filter(f => f.family === 'Inter Variable' && f.status === 'loaded').length)`,
      awaitPromise: true, returnByValue: true,
    }, sessionId);
    if (!result.value) throw new Error(`Inter Variable did not load for ${path.basename(file)} — check report/assets/`);
    await sleep(250);   // let the cover gradient raster settle

    const { data } = await cdp.send('Page.printToPDF', {
      ...A4, printBackground: true, preferCSSPageSize: false, scale: 1, ...printOptions,
    }, sessionId);
    return Buffer.from(data, 'base64');
  } finally {
    await cdp.send('Target.closeTarget', { targetId }).catch(() => {});
  }
}

/** First python that can import pypdf. */
function findPython() {
  const candidates = [
    path.join(HERE, '.venv/bin/python'),
    process.env.PYTHON,
    'python3.12',
    'python3',
  ].filter(Boolean);
  for (const py of candidates) {
    const r = spawnSync(py, ['-c', 'import pypdf'], { stdio: 'ignore' });
    if (r.status === 0) return py;
  }
  return null;
}

async function main() {
  const chrome = CHROME_CANDIDATES.find(existsSync);
  if (!chrome) throw new Error('No Chrome/Chromium found. Install Google Chrome.');
  for (const f of [COVER, BODY]) if (!existsSync(f)) throw new Error(`Missing ${f}`);

  const python = findPython();
  if (!python) {
    throw new Error('No Python with pypdf (needed to join cover + body). Run:\n' +
      '  python3.12 -m venv report/.venv && report/.venv/bin/pip install pypdf');
  }

  const profile = await mkdtemp(path.join(tmpdir(), 'sc4es-pdf-'));
  const proc = spawn(chrome, [
    '--headless=new',
    '--disable-gpu',
    '--hide-scrollbars',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-extensions',
    '--allow-file-access-from-files',   // local @font-face + <img> off file://
    '--font-render-hinting=none',
    '--remote-debugging-port=0',
    `--user-data-dir=${profile}`,
    'about:blank',
  ], { stdio: ['ignore', 'ignore', 'pipe'] });

  // Chrome writes the chosen port to <profile>/DevToolsActivePort once it is up.
  let port = null;
  for (let i = 0; i < 120 && port === null; i++) {
    try {
      const first = (await readFile(path.join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0].trim();
      if (first) port = Number(first);
    } catch { await sleep(100); }
  }
  if (!port) { proc.kill(); throw new Error('Chrome did not expose a debugging port'); }

  const { webSocketDebuggerUrl } = await (await fetch(`http://127.0.0.1:${port}/json/version`)).json();
  const cdp = await CDP.connect(webSocketDebuggerUrl);

  const coverPdf = path.join(profile, 'cover.pdf');
  const bodyPdf = path.join(profile, 'body.pdf');
  try {
    await writeFile(coverPdf, await print(cdp, COVER, {
      marginTop: 0, marginBottom: 0, marginLeft: 0, marginRight: 0,
      displayHeaderFooter: false,
      pageRanges: '1',
    }));
    await writeFile(bodyPdf, await print(cdp, BODY, {
      marginTop: 17 * MM, marginBottom: 16 * MM, marginLeft: 0, marginRight: 0,
      displayHeaderFooter: true, headerTemplate, footerTemplate,
    }));
  } finally {
    cdp.close();
    proc.kill();
  }

  const merge = spawnSync(python, ['-c', `
import sys
from pypdf import PdfWriter
w = PdfWriter()
for f in sys.argv[1:-1]:
    w.append(f)
w.add_metadata({
    '/Title': 'SoftClient4ES vs Trino — Elasticsearch extraction benchmark',
    '/Author': 'SOFTNETWORK',
    '/Subject': 'Extracting 10M rows out of Elasticsearch: Arrow Flight SQL vs the Trino '
                'Elasticsearch connector, measured on the released 0.2.5.1 build',
    '/Keywords': 'Elasticsearch, Arrow Flight SQL, Trino, benchmark, SoftClient4ES',
    '/Creator': 'softclient4es.dev',
})
with open(sys.argv[-1], 'wb') as fh:
    w.write(fh)
print(len(w.pages))
`, coverPdf, bodyPdf, OUTPUT], { encoding: 'utf8' });
  if (merge.status !== 0) throw new Error(`pypdf join failed: ${merge.stderr}`);

  await rm(profile, { recursive: true, force: true }).catch(() => {});
  console.log(`✓ ${OUTPUT}  (${merge.stdout.trim()} pages)`);
}

main().catch((e) => { console.error('✗', e.message); process.exit(1); });
