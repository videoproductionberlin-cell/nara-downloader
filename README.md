# NARA Catalog Document Downloader

Downloads complete multi-page documents from the [National Archives Catalog](https://catalog.archives.gov/) and combines them into a single PDF — with optional OCR to make scanned documents searchable.

## The Problem

Many documents on catalog.archives.gov can only be viewed page-by-page in the browser. There's no "download full document" button. This tool automates downloading every page and merging them into one PDF.

## Features

- Downloads **all pages** of a document automatically
- **Concurrent downloads** for speed (configurable thread count)
- Combines them into a **single PDF** using lossless `img2pdf` (no re-encoding)
- Optional **OCR** with Tesseract (adds a searchable text layer)
- Supports **multiple OCR languages** (English, German, etc.)
- Extracts **existing NARA OCR text** if available
- Polite rate-limiting and retry logic
- Works with NARA Catalog URLs or plain NAID numbers

## Quick Start

### With Docker (recommended)

```bash
# Build the image
docker build -t nara-downloader .

# Download a document (output goes to current directory)
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader https://catalog.archives.gov/id/1667751

# Download with OCR
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader https://catalog.archives.gov/id/1667751 --ocr

# Download with German + English OCR
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader 1667751 --ocr --lang deu+eng

# Just download page images (no PDF)
docker run --rm -u $(id -u):$(id -g) -v $(pwd):/output nara-downloader 1667751 --images-only
```

> **Note:** The `-u $(id -u):$(id -g)` flag ensures output files are owned by your user instead of root.

### Without Docker

```bash
# Install system dependencies (Ubuntu/Debian)
sudo apt-get install tesseract-ocr tesseract-ocr-eng tesseract-ocr-deu \
    ghostscript poppler-utils

# Install Python dependencies
pip install -r requirements.txt
pip install ocrmypdf pikepdf pdf2image pytesseract  # optional, for OCR and PDF merging

# Run
python nara_download.py https://catalog.archives.gov/id/1667751
python nara_download.py 1667751 --ocr --lang eng
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
  --delay SECONDS        Delay between page downloads (default: 0.3)
  --workers N            Number of concurrent download threads (default: 4)
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

# Faster downloads with more threads
python nara_download.py 1667751 --workers 8
```

## How It Works

1. Takes a NARA Catalog URL or NAID number
2. Queries the NARA Catalog API (proxy endpoints) to get record metadata
3. Extracts all digital object (page) URLs from the record
4. Downloads each page concurrently via direct S3 URLs at full resolution
5. Combines all pages into a single PDF using `img2pdf` (lossless, memory-efficient)
6. Optionally adds a searchable OCR text layer using ocrmypdf/Tesseract

## OCR Details

The tool supports two OCR approaches:

1. **ocrmypdf** (recommended): Creates a proper searchable PDF with an invisible text layer overlaid on the original images. Includes deskewing, cleaning, and optimization. Install with `pip install ocrmypdf`.

2. **pytesseract** (fallback): Extracts text to a separate `.txt` file. Less integrated but works without ocrmypdf.

3. **NARA existing text**: Many records already have OCR text in the NARA metadata. Use `--extract-text` to save it without running your own OCR.

### Installing additional OCR languages

```bash
# List available languages
apt-cache search tesseract-ocr-

# Install German
sudo apt-get install tesseract-ocr-deu

# Docker image includes eng and deu by default
```

## Rate Limiting

The tool includes a configurable delay between downloads (default 0.3s) and automatic retry with backoff on rate-limit (HTTP 429) or server errors. Adjust with `--delay` if needed. Concurrent downloads default to 4 threads — increase with `--workers` for faster downloads, or decrease to be more polite to NARA's servers.

## Requirements

- Python 3.9+
- For OCR: Tesseract, ocrmypdf
- For mixed PDF merging: pikepdf
- See `requirements.txt` for Python packages

## License

MIT
