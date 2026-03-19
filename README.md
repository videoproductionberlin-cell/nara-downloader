# NARA Catalog Document Downloader

Downloads complete multi-page documents from the [National Archives Catalog](https://catalog.archives.gov/) and combines them into a single PDF — with optional OCR to make scanned documents searchable.

Optimized for large documents (1,000+ pages) with fully async I/O, lossless PDF creation, and memory-safe processing.

## The Problem

Many documents on catalog.archives.gov can only be viewed page-by-page in the browser. There's no "download full document" button. This tool automates downloading every page and merging them into one PDF.

## Features

- Downloads **all pages** of a document automatically
- **Fully async** — single `aiohttp` network stack, no threads, no GIL contention
- Optional **uvloop** for even faster async performance on macOS/Linux
- Combines pages into a **single PDF** using lossless `img2pdf` (no re-encoding, near-zero memory)
- **Chunked fallback** — if `img2pdf` can't handle a format, Pillow processes in 50-page batches to prevent OOM
- Optional **OCR** with Tesseract (adds a searchable text layer)
- Supports **multiple OCR languages** (English, German, etc.)
- Extracts **existing NARA OCR text** if available
- Proper `logging` module — control output with `--verbose` / `--quiet`
- **Importable as a library** — `NaraClient` class with async context manager
- Polite rate-limiting and retry logic with exponential backoff
- Works with NARA Catalog URLs or plain NAID numbers

## Quick Start

### With Docker

```bash
# Build the image (use --platform for native Apple Silicon)
docker build --platform linux/arm64 -t nara-downloader .

# Download a document
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader https://catalog.archives.gov/id/1667751

# Download with OCR
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader https://catalog.archives.gov/id/1667751 --ocr

# Download with German + English OCR
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader 1667751 --ocr --lang deu+eng

# Just download page images (no PDF)
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader 1667751 --images-only
```

> **Note:** The `-u $(id -u):$(id -g)` flag ensures output files are owned by your user instead of root.

### Native on macOS (fastest, recommended for Apple Silicon)

For the absolute best performance on M1/M2/M3 Macs, skip Docker and run natively. This gives Tesseract and ocrmypdf direct access to all ARM64 performance cores.

```bash
# Install system dependencies via Homebrew
brew install tesseract tesseract-lang ocrmypdf ghostscript poppler

# Install Python dependencies
pip install -r requirements.txt
pip install -r requirements-optional.txt  # OCR + uvloop

# Run
python nara_download.py 1667751 --ocr --concurrent 30
```

### On Linux (Ubuntu/Debian)

```bash
# Install system dependencies
sudo apt-get install tesseract-ocr tesseract-ocr-eng tesseract-ocr-deu \
    ghostscript poppler-utils

# Install Python dependencies
pip install -r requirements.txt
pip install -r requirements-optional.txt  # OCR + uvloop

# Run
python nara_download.py https://catalog.archives.gov/id/1667751
```

## Usage

```
python nara_download.py <URL or NAID> [options]

Arguments:
  target                 NARA Catalog URL or NAID number

Options:
  -o, --output FILE      Output PDF filename (auto-generated from title by default)
  --ocr                  Add OCR text layer (requires tesseract)
  --lang LANG            OCR language(s): eng, deu, fra, deu+eng, etc. (default: eng)
  --keep-images          Keep downloaded page images alongside the PDF
  --images-only          Download page images only, don't create PDF
  --output-dir DIR       Output directory (default: current directory)
  --extract-text         Save NARA's existing OCR text to a .txt file
  --delay SECONDS        Delay between downloads in seconds (default: 0.1)
  --concurrent N         Max concurrent downloads (default: 20)
  -v, --verbose          Show debug output
  -q, --quiet            Only show warnings and errors
```

## Examples

```bash
# Basic: download and create PDF
python nara_download.py https://catalog.archives.gov/id/1667751

# With OCR in English
python nara_download.py 1667751 --ocr

# With OCR in German and English
python nara_download.py 1667751 --ocr --lang deu+eng

# Custom output filename
python nara_download.py 1667751 -o "my_document.pdf"

# Download to specific directory
python nara_download.py 1667751 --output-dir ~/nara-docs/

# Keep individual page images
python nara_download.py 1667751 --keep-images

# Extract NARA's existing OCR text
python nara_download.py 1667751 --extract-text

# Images only (no PDF)
python nara_download.py 1667751 --images-only --output-dir ./pages/

# Max throughput for a huge document
python nara_download.py 1667751 --concurrent 50 --delay 0.05

# Quiet mode (only warnings and errors)
python nara_download.py 1667751 -q

# Debug mode (verbose output)
python nara_download.py 1667751 -v
```

## Library Usage

The downloader can be imported and used programmatically:

```python
import asyncio
from nara_download import NaraClient, get_digital_objects, prepare_images, compile_pdf

async def download_document(naid: str, output_dir: str = "."):
    async with NaraClient(max_concurrent=30) as client:
        record = await client.fetch_record(naid)
        objects = get_digital_objects(record)
        paths = await client.download_pages(objects, output_dir)

        prepared = prepare_images(paths)
        compile_pdf(prepared, f"{output_dir}/output.pdf")

asyncio.run(download_document("595500", "/tmp/nara"))
```

## How It Works

1. Takes a NARA Catalog URL or NAID number
2. Queries the NARA Catalog API (proxy endpoints) to get record metadata
3. Extracts all digital object (page) URLs from the record
4. Downloads pages concurrently via async I/O (`aiohttp` + semaphore-based throttling)
5. Normalizes all files via `prepare_images()` (separates images from PDFs, converts unsupported formats)
6. Assembles the final PDF via `compile_pdf()` using `img2pdf` (lossless, memory-efficient)
7. Optionally adds a searchable OCR text layer using ocrmypdf/Tesseract

## Performance

### Download speed

The entire network stack is async — metadata fetching and image downloads all run through a single `aiohttp.ClientSession`. A semaphore controls concurrency without OS threads or GIL contention. If `uvloop` is installed, it replaces Python's default event loop for additional throughput.

| Pages | Threads (v1) | Async (v2+)      |
|-------|-------------|------------------|
| 100   | ~45s        | ~12s             |
| 500   | ~4 min      | ~50s             |
| 1,000 | ~8 min      | ~1.5 min         |

*(Estimates at --concurrent 20, --delay 0.1, typical NARA response times)*

### Memory usage

- **img2pdf** (primary): Wraps JPEG/PNG streams directly into PDF without decompression. Memory usage is constant regardless of page count.
- **Pillow fallback**: If img2pdf fails, images are processed in chunks of 50 pages. Each chunk is saved to a temp PDF and freed from RAM before the next chunk starts. All chunks are merged at the end via pikepdf.

### Apple Silicon optimization

Docker on macOS can run images through Rosetta 2 (x86_64 translation), which dramatically slows OCR. To ensure native ARM64 execution:

```bash
# Force ARM64 build
docker build --platform linux/arm64 -t nara-downloader .
```

For maximum performance, run natively via Homebrew — this gives Tesseract and ocrmypdf direct access to all Apple Silicon performance cores without any translation layer.

## OCR Details

The tool supports two OCR approaches:

1. **ocrmypdf** (recommended): Creates a proper searchable PDF with an invisible text layer overlaid on the original images. Includes deskewing, cleaning, and optimization. Automatically uses all available CPU cores. Install with `pip install ocrmypdf`.

2. **pytesseract** (fallback): Extracts text to a separate `.txt` file. Less integrated but works without ocrmypdf.

3. **NARA existing text**: Many records already have OCR text in the NARA metadata. Use `--extract-text` to save it without running your own OCR.

### Installing additional OCR languages

```bash
# macOS (Homebrew)
brew install tesseract-lang   # installs all languages

# Ubuntu/Debian
apt-cache search tesseract-ocr-
sudo apt-get install tesseract-ocr-deu

# Docker image includes eng and deu by default
```

## Rate Limiting

The tool uses a semaphore to cap concurrent connections (default 20) and adds a configurable delay between downloads (default 0.1s). Automatic retry with exponential backoff handles HTTP 429 (rate limit) and server errors. Adjust `--concurrent` and `--delay` to balance speed vs. politeness to NARA's servers.

## Requirements

- Python 3.9+
- Core: `aiohttp`, `Pillow`, `img2pdf`, `tqdm`, `pikepdf` (see `requirements.txt`)
- OCR: `ocrmypdf`, `pytesseract`, `pdf2image`, plus Tesseract system package
- Optional: `uvloop` for faster async on macOS/Linux
- See `requirements-optional.txt` for OCR and performance extras

## License

MIT
