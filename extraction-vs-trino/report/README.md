# PDF edition of the benchmark results

`SoftClient4ES-vs-Trino-Extraction-Benchmark.pdf` — a print-ready A4 report carrying the figures
from [RESULTS.md](../RESULTS.md), with [METHODOLOGY.md](../METHODOLOGY.md) and
[FINDINGS.md](../FINDINGS.md) as appendices A and B.

Every number in it is copied from those documents; the PDF adds no measurement of its own. If a
figure changes upstream, change it here too — nothing is generated from the markdown.

## Brand

Colours and type come from the website (`softclient4es-web`):

| Token | Value | Source |
|---|---|---|
| `--sc-blue` / `--sc-blue-light` / `--sc-blue-dark` | `#1e40af` / `#3b82f6` / `#1e3a8a` | `src/styles/global.css` |
| `--sc-green` / `--sc-orange` | `#059669` / `#ea580c` | idem |
| `--sc-teal` (appendices) | `#0f766e` | Starlight docs accent |
| Type | Inter Variable | `@fontsource-variable/inter` |
| Logo | `assets/logo-mark.png` | `public/images/logo-mark.png` |

Chart marks use `#2563eb` (SoftClient4ES) and `#ea580c` (Trino) rather than the darker `--sc-blue`:
that pair passes the lightness-band, chroma, CVD-separation and contrast checks for a two-series
categorical palette, which `#1e40af` fails on lightness. Headings and UI chrome keep `--sc-blue`.

To re-sync the assets after a website change:

```bash
W=../../../softclient4es-web
cp $W/public/images/logo-mark.png assets/
cp $W/node_modules/@fontsource-variable/inter/files/inter-latin-wght-{normal,italic}.woff2 assets/
```

## Files

| File | Role |
|---|---|
| `cover.html` | the cover, printed full-bleed in its own pass |
| `report.html` | the body |
| `report.css` | shared styles (design tokens, tables, figures, print rules) |
| `build.mjs` | drives headless Chrome and joins the two passes |
| `assets/` | logo + Inter woff2, referenced relatively |

## Rebuilding

Needs Google Chrome (or Chromium/Edge), Node ≥ 20, and a Python with `pypdf`:

```bash
python3.12 -m venv .venv && .venv/bin/pip install pypdf   # once
node build.mjs                                            # → SoftClient4ES-vs-Trino-...pdf
```

`build.mjs` prints in two passes because Chrome's DevTools print API ignores `@page :first` and
draws its running header/footer inside the page margins: the cover is printed with zero margins
(full bleed, unnumbered) and the body with 17/16 mm margins plus the running header and footer, then
the two are concatenated. Body pages are numbered 1..N with the cover unnumbered, so the footer's
`n / N` counts body pages only.

Chrome is launched with `--allow-file-access-from-files` so the `file://` page can load the local
Inter woff2; the build fails loudly if Inter did not resolve, rather than shipping a PDF silently
set in the system sans.
