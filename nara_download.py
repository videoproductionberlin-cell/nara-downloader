#!/usr/bin/env python3
"""
NARA Catalog Document Downloader
=================================
Downloads all pages of a document from the National Archives Catalog
(catalog.archives.gov) and combines them into a single PDF with
optional OCR (searchable text layer).

Usage:
    python nara_download.py <URL or NAID> [options]

Examples:
    python nara_download.py https://catalog.archives.gov/id/595500
    python nara_download.py 595500
    python nara_download.py 595500 --ocr
    python nara_download.py 595500 --ocr --lang deu
    python nara_download.py 595500 --output my_document.pdf
"""

import argparse
import os
import re
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import img2pdf
import requests
from PIL import Image
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROXY_SEARCH = "https://catalog.archives.gov/proxy/v3/records/search"
PROXY_SEARCH_V2 = "https://catalog.archives.gov/proxy/records/search"
IIIF_BASE = "https://catalog.archives.gov/iiif/3"
CHILD_RECORDS = "https://catalog.archives.gov/proxy/records/parentNaId"

USER_AGENT = (
    "NARA-Downloader/1.0 "
    "(Document research tool; contact: github.com/nara-downloader)"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
})

# Retry logic
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Concurrent downloads
DEFAULT_MAX_WORKERS = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_naid(url_or_id: str) -> str:
    """Extract the NAID from a URL like catalog.archives.gov/id/595500 or a plain number."""
    url_or_id = url_or_id.strip().rstrip("/")
    # Plain number
    if url_or_id.isdigit():
        return url_or_id
    # URL pattern: /id/12345 or /id/12345?...
    m = re.search(r"/id/(\d+)", url_or_id)
    if m:
        return m.group(1)
    # Fallback: last numeric segment
    parts = urlparse(url_or_id).path.strip("/").split("/")
    for part in reversed(parts):
        if part.isdigit():
            return part
    print(f"[ERROR] Could not extract NAID from: {url_or_id}", file=sys.stderr)
    sys.exit(1)


def api_get(url: str, params: dict = None) -> dict:
    """GET request with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = RETRY_DELAY * attempt * 2
                print(f"  Rate-limited. Waiting {wait}s …")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                print(f"  Server error ({resp.status_code}). Retry {attempt}/{MAX_RETRIES} …")
                time.sleep(RETRY_DELAY * attempt)
                continue
            # Other client errors
            print(f"[ERROR] HTTP {resp.status_code} for {url}", file=sys.stderr)
            print(f"  Response: {resp.text[:500]}", file=sys.stderr)
            return {}
        except requests.exceptions.RequestException as e:
            print(f"  Connection error: {e}. Retry {attempt}/{MAX_RETRIES} …")
            time.sleep(RETRY_DELAY * attempt)
    print(f"[ERROR] Failed after {MAX_RETRIES} attempts: {url}", file=sys.stderr)
    return {}


def download_image(url: str, dest: str) -> bool:
    """Download an image file with retry logic. Returns True on success."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=60, stream=True)
            if resp.status_code == 200:
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            if resp.status_code == 429:
                wait = RETRY_DELAY * attempt * 3
                print(f"  Rate-limited downloading image. Waiting {wait}s …")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                time.sleep(RETRY_DELAY * attempt)
                continue
            # Try direct S3 URL as fallback
            if attempt == MAX_RETRIES and "iiif" in url:
                return False
            print(f"  HTTP {resp.status_code} downloading image. Retry {attempt}/{MAX_RETRIES} …")
            time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            print(f"  Download error: {e}. Retry {attempt}/{MAX_RETRIES} …")
            time.sleep(RETRY_DELAY * attempt)
    return False


# ---------------------------------------------------------------------------
# Core: Fetch record metadata
# ---------------------------------------------------------------------------
def fetch_record(naid: str) -> dict:
    """Fetch a record by NAID. Returns the record dict or {}."""
    print(f"Fetching record metadata for NAID {naid} …")

    # Try v3 proxy endpoint first
    data = api_get(PROXY_SEARCH, params={
        "naId_is": naid,
        "allowLegacyOrgNames": "true",
        "includeExtractedText": "true",
        "includeOtherExtractedText": "true",
    })

    hits = _extract_hits(data)
    if hits:
        return hits[0]

    # Fallback to v2 proxy
    print("  v3 endpoint returned no results, trying v2 …")
    data = api_get(PROXY_SEARCH_V2, params={"naId": naid})
    hits = _extract_hits(data)
    if hits:
        return hits[0]

    print(f"[ERROR] No record found for NAID {naid}", file=sys.stderr)
    return {}


def _extract_hits(data: dict) -> list:
    """Extract hit records from the API response."""
    if not data:
        return []
    # v3 format: body.hits.hits[]._source.record
    body = data.get("body", data)
    hits_obj = body.get("hits", {})
    hits = hits_obj.get("hits", [])
    records = []
    for hit in hits:
        source = hit.get("_source", hit)
        record = source.get("record", source)
        if record:
            records.append(record)
    return records


def get_digital_objects(record: dict) -> list:
    """Extract digital objects (pages) from a record, sorted by page number."""
    objects = record.get("digitalObjects", [])
    if not objects:
        # Sometimes nested differently
        objects = record.get("objects", [])
    if not objects:
        return []

    # Sort by pageNum or objectDesignator
    def sort_key(obj):
        pn = obj.get("pageNum")
        if pn is not None:
            try:
                return int(pn)
            except (ValueError, TypeError):
                pass
        des = obj.get("objectDesignator", "0")
        try:
            return int(des)
        except (ValueError, TypeError):
            return 0

    objects.sort(key=sort_key)
    return objects


def fetch_child_file_units(naid: str) -> list:
    """
    For series/record groups, fetch child file units that contain
    the actual digital objects.
    """
    print(f"  Checking for child records under NAID {naid} …")
    all_children = []
    page = 1
    while True:
        data = api_get(f"{CHILD_RECORDS}/{naid}", params={
            "abbreviated": "true",
            "limit": 50,
            "sort": "naId:asc",
            "page": page,
        })
        body = data.get("body", data)
        hits = body.get("hits", {}).get("hits", [])
        if not hits:
            break
        for hit in hits:
            source = hit.get("_source", hit)
            record = source.get("record", source)
            if record:
                all_children.append(record)
        total = body.get("hits", {}).get("total", {})
        total_val = total.get("value", 0) if isinstance(total, dict) else total
        if len(all_children) >= int(total_val):
            break
        page += 1
    return all_children


# ---------------------------------------------------------------------------
# Core: Download all pages (concurrent)
# ---------------------------------------------------------------------------
def download_all_pages(
    objects: list,
    tmp_dir: str,
    naid: str,
    delay: float = 0.3,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> list:
    """
    Download all digital object images to tmp_dir using concurrent requests.
    Returns list of local file paths in page order.
    """
    if not objects:
        print("[ERROR] No digital objects found for this record.", file=sys.stderr)
        return []

    print(f"Found {len(objects)} page(s). Downloading ({max_workers} threads) …\n")

    def _fetch_page(index: int, obj: dict) -> tuple:
        """Download a single page. Returns (sort_key, local_path) or (sort_key, None)."""
        object_url = obj.get("objectUrl", "")
        if not object_url:
            return (index, None)

        page_num = obj.get("pageNum", index + 1)
        ext = Path(object_url).suffix.lower() or ".jpg"
        local_path = os.path.join(tmp_dir, f"page_{page_num:05d}{ext}")

        success = download_image(object_url, local_path)
        if delay > 0:
            time.sleep(delay)
        return (index, local_path if success else None)

    downloaded = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_page, i, obj): i
            for i, obj in enumerate(objects)
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Downloading pages",
            unit="page",
        ):
            index, path = future.result()
            if path:
                downloaded[index] = path
            else:
                page_num = objects[futures[future]].get("pageNum", futures[future] + 1)
                tqdm.write(f"  [WARN] Failed to download page {page_num}")

    # Return paths sorted by original page order
    return [downloaded[i] for i in sorted(downloaded)]


# ---------------------------------------------------------------------------
# Core: Combine into PDF
# ---------------------------------------------------------------------------
def images_to_pdf(image_paths: list, output_path: str) -> str:
    """
    Combine a list of image files into a single PDF.
    Uses img2pdf for lossless, memory-efficient JPEG/PNG wrapping.
    Falls back to pikepdf for mixed image+PDF inputs.
    """
    if not image_paths:
        print("[ERROR] No files to combine.", file=sys.stderr)
        return ""

    print(f"\nCombining {len(image_paths)} page(s) into PDF …")

    # Separate image files from PDF files
    image_files = []
    pdf_files = []
    for path in image_paths:
        ext = Path(path).suffix.lower()
        if ext == ".pdf":
            pdf_files.append(path)
        elif ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".gif"):
            image_files.append(path)
        else:
            # Try to open with Pillow and convert to JPEG for img2pdf
            try:
                img = Image.open(path)
                converted = path + ".jpg"
                img.convert("RGB").save(converted, "JPEG", quality=95)
                image_files.append(converted)
                img.close()
            except Exception as e:
                print(f"  [WARN] Could not process {path}: {e}")

    if not image_files and not pdf_files:
        print("[ERROR] No valid files to combine.", file=sys.stderr)
        return ""

    # Simple case: all images, no PDFs — use img2pdf (fast, lossless, low memory)
    if not pdf_files:
        try:
            with open(output_path, "wb") as f:
                f.write(img2pdf.convert(image_files))
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"PDF saved: {output_path} ({size_mb:.1f} MB)")
            return output_path
        except Exception as e:
            print(f"  [WARN] img2pdf failed: {e}")
            print("  Falling back to Pillow …")
            return _pillow_images_to_pdf(image_files, output_path)

    # Mixed case: images + PDFs — use pikepdf to merge
    return _merge_mixed_to_pdf(image_files, pdf_files, output_path)


def _pillow_images_to_pdf(image_paths: list, output_path: str) -> str:
    """Fallback: use Pillow to create PDF (higher memory usage, re-encodes images)."""
    if not image_paths:
        return ""

    # Process one image at a time to limit memory usage
    first_img = Image.open(image_paths[0])
    if first_img.mode != "RGB":
        first_img = first_img.convert("RGB")

    rest_images = []
    for path in image_paths[1:]:
        try:
            img = Image.open(path)
            if img.mode != "RGB":
                img = img.convert("RGB")
            rest_images.append(img)
        except Exception as e:
            print(f"  [WARN] Could not process {path}: {e}")

    first_img.save(output_path, "PDF", save_all=True, append_images=rest_images, resolution=300)

    # Close all images to free memory
    first_img.close()
    for img in rest_images:
        img.close()

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"PDF saved: {output_path} ({size_mb:.1f} MB)")
    return output_path


def _merge_mixed_to_pdf(image_files: list, pdf_files: list, output_path: str) -> str:
    """
    Merge a mix of image files and PDF files into one PDF.
    Uses img2pdf for images and pikepdf for merging.
    """
    try:
        import pikepdf
    except ImportError:
        print("  [WARN] pikepdf not installed — skipping embedded PDF pages.")
        print("  Install with: pip install pikepdf")
        if image_files:
            return images_to_pdf(image_files, output_path)
        return ""

    temp_pdfs = []

    # Convert all images to a single PDF via img2pdf
    if image_files:
        images_pdf = output_path + ".tmp_images.pdf"
        try:
            with open(images_pdf, "wb") as f:
                f.write(img2pdf.convert(image_files))
            temp_pdfs.append(images_pdf)
        except Exception as e:
            print(f"  [WARN] img2pdf failed for images: {e}")

    # Add existing PDF files
    temp_pdfs.extend(pdf_files)

    # Merge all PDFs
    merged = pikepdf.Pdf.new()
    for pdf_path in temp_pdfs:
        try:
            src = pikepdf.open(pdf_path)
            merged.pages.extend(src.pages)
        except Exception as e:
            print(f"  [WARN] Could not merge {pdf_path}: {e}")
    merged.save(output_path)
    merged.close()

    # Cleanup temp files
    for path in temp_pdfs:
        if ".tmp_" in path:
            try:
                os.remove(path)
            except OSError:
                pass

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"PDF saved: {output_path} ({size_mb:.1f} MB)")
    return output_path


# ---------------------------------------------------------------------------
# Core: OCR
# ---------------------------------------------------------------------------
def add_ocr_layer(input_pdf: str, output_pdf: str, lang: str = "eng") -> str:
    """
    Add a searchable text layer to the PDF using ocrmypdf (which uses
    Tesseract under the hood). Falls back to pytesseract if ocrmypdf
    is not available.
    """
    print(f"\nAdding OCR text layer (language: {lang}) …")

    # Try ocrmypdf first (best quality, preserves layout)
    try:
        import ocrmypdf

        # Check optional dependencies
        has_unpaper = shutil.which("unpaper") is not None
        has_gs = shutil.which("gs") is not None

        kwargs = dict(
            language=lang,
            deskew=True,
            optimize=1 if has_gs else 0,
            skip_text=True,  # Don't re-OCR pages that already have text
            progress_bar=True,
        )
        if has_unpaper:
            kwargs["clean"] = True

        ocrmypdf.ocr(input_pdf, output_pdf, **kwargs)
        size_mb = os.path.getsize(output_pdf) / (1024 * 1024)
        print(f"OCR PDF saved: {output_pdf} ({size_mb:.1f} MB)")
        return output_pdf
    except ImportError:
        pass
    except Exception as e:
        print(f"  ocrmypdf failed: {e}")

    # Fallback: pytesseract + basic text extraction
    try:
        import pytesseract

        print("  Using pytesseract fallback (text extraction only) …")
        full_text = []
        pdf_images = _pdf_to_images(input_pdf)
        for i, img in enumerate(tqdm(pdf_images, desc="OCR processing", unit="page")):
            text = pytesseract.image_to_string(img, lang=lang)
            full_text.append(f"--- Page {i+1} ---\n{text}")

        # Save extracted text alongside the PDF
        text_path = input_pdf.replace(".pdf", "_ocr_text.txt")
        with open(text_path, "w", encoding="utf-8") as f:
            f.write("\n\n".join(full_text))
        print(f"  OCR text saved: {text_path}")

        # Copy the original PDF (pytesseract alone can't embed text in PDF easily)
        shutil.copy2(input_pdf, output_pdf)
        print(f"  PDF copied (text layer requires ocrmypdf): {output_pdf}")
        return output_pdf

    except ImportError:
        print("[ERROR] Neither ocrmypdf nor pytesseract is available.", file=sys.stderr)
        print("  Install one of:", file=sys.stderr)
        print("    pip install ocrmypdf    (recommended, full PDF OCR)", file=sys.stderr)
        print("    pip install pytesseract  (text extraction only)", file=sys.stderr)
        return input_pdf


def _pdf_to_images(pdf_path: str) -> list:
    """Convert PDF pages to PIL Images for OCR processing."""
    try:
        from pdf2image import convert_from_path
        return convert_from_path(pdf_path, dpi=300)
    except ImportError:
        print("  [WARN] pdf2image not installed, trying to OCR from original images")
        return []


# ---------------------------------------------------------------------------
# Core: Extract existing OCR text from NARA
# ---------------------------------------------------------------------------
def extract_nara_text(record: dict, objects: list) -> str:
    """Extract any OCR text that NARA already has for this record."""
    texts = []
    for obj in objects:
        text = obj.get("extractedText", "")
        if text:
            page = obj.get("pageNum", "?")
            texts.append(f"--- Page {page} ---\n{text}")
    return "\n\n".join(texts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def sanitize_filename(title: str) -> str:
    """Create a safe filename from a record title."""
    # Remove problematic characters
    safe = re.sub(r'[<>:"/\\|?*]', '', title)
    safe = re.sub(r'\s+', '_', safe.strip())
    # Truncate
    if len(safe) > 100:
        safe = safe[:100]
    return safe or "nara_document"


def main():
    parser = argparse.ArgumentParser(
        description="Download complete documents from the National Archives Catalog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s https://catalog.archives.gov/id/595500
  %(prog)s 595500
  %(prog)s 595500 --ocr
  %(prog)s 595500 --ocr --lang deu+eng
  %(prog)s 595500 --output my_document.pdf
  %(prog)s 595500 --keep-images
        """,
    )
    parser.add_argument(
        "target",
        help="NARA Catalog URL or NAID number",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output PDF filename (default: auto-generated from title)",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Add OCR text layer to the PDF (requires tesseract)",
    )
    parser.add_argument(
        "--lang",
        default="eng",
        help="OCR language(s), e.g. 'eng', 'deu', 'deu+eng' (default: eng)",
    )
    parser.add_argument(
        "--keep-images",
        action="store_true",
        help="Keep downloaded page images after creating PDF",
    )
    parser.add_argument(
        "--images-only",
        action="store_true",
        help="Download individual page images without creating a PDF",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Output directory (default: current directory)",
    )
    parser.add_argument(
        "--extract-text",
        action="store_true",
        help="Extract NARA's existing OCR text (if available) to a .txt file",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Delay between page downloads in seconds (default: 0.3)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Number of concurrent download threads (default: {DEFAULT_MAX_WORKERS})",
    )

    args = parser.parse_args()

    naid = extract_naid(args.target)
    print(f"{'='*60}")
    print(f" NARA Catalog Document Downloader")
    print(f" NAID: {naid}")
    print(f" URL:  https://catalog.archives.gov/id/{naid}")
    print(f"{'='*60}\n")

    # Fetch record
    record = fetch_record(naid)
    if not record:
        sys.exit(1)

    title = record.get("title", f"NAID_{naid}")
    level = record.get("levelOfDescription", "unknown")
    print(f"Title: {title}")
    print(f"Level: {level}\n")

    # Get digital objects
    objects = get_digital_objects(record)

    if not objects and level in ("series", "recordGroup", "collection"):
        # This is a container record — fetch child items
        print("This is a container record (series/collection).")
        children = fetch_child_file_units(naid)
        if children:
            print(f"Found {len(children)} child record(s).")
            print("Downloading the first child record. For bulk downloads, run")
            print("this tool once per child NAID.\n")
            for child in children[:5]:
                child_naid = child.get("naId", "")
                child_title = child.get("title", "untitled")
                print(f"  - NAID {child_naid}: {child_title}")
            if len(children) > 5:
                print(f"  … and {len(children) - 5} more")
            print()

            # Download the first child as a demo
            if children:
                first_child = children[0]
                child_objects = get_digital_objects(first_child)
                if child_objects:
                    objects = child_objects
                    title = first_child.get("title", title)
                    naid = str(first_child.get("naId", naid))
                    print(f"Downloading: {title} (NAID {naid})\n")
                else:
                    # Fetch full record for the child
                    child_naid = str(first_child.get("naId", ""))
                    if child_naid:
                        child_record = fetch_record(child_naid)
                        if child_record:
                            child_objects = get_digital_objects(child_record)
                            if child_objects:
                                objects = child_objects
                                title = child_record.get("title", title)
                                naid = child_naid

    if not objects:
        print("[ERROR] No downloadable digital objects found for this record.")
        print("        The record may not have been digitized, or may use a")
        print("        different format (e.g. electronic records/data files).")
        sys.exit(1)

    # Extract existing NARA OCR text if requested
    if args.extract_text:
        nara_text = extract_nara_text(record, objects)
        if nara_text:
            text_filename = sanitize_filename(title) + "_nara_ocr.txt"
            text_path = os.path.join(args.output_dir, text_filename)
            os.makedirs(args.output_dir, exist_ok=True)
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(nara_text)
            print(f"NARA OCR text saved: {text_path}\n")
        else:
            print("No existing OCR text found in NARA metadata.\n")

    # Prepare output directory
    os.makedirs(args.output_dir, exist_ok=True)
    safe_title = sanitize_filename(title)

    # Create temp directory for downloads
    with tempfile.TemporaryDirectory(prefix="nara_") as tmp_dir:
        # Download all pages
        downloaded = download_all_pages(
            objects, tmp_dir, naid,
            delay=args.delay,
            max_workers=args.workers,
        )

        if not downloaded:
            print("[ERROR] No pages were downloaded successfully.", file=sys.stderr)
            sys.exit(1)

        print(f"\nSuccessfully downloaded {len(downloaded)}/{len(objects)} page(s).")

        # Images-only mode
        if args.images_only:
            dest_dir = os.path.join(args.output_dir, f"{safe_title}_pages")
            os.makedirs(dest_dir, exist_ok=True)
            for src in downloaded:
                shutil.copy2(src, dest_dir)
            print(f"\nImages saved to: {dest_dir}/")
            return

        # Combine into PDF
        output_name = args.output or f"{safe_title}.pdf"
        if not output_name.lower().endswith(".pdf"):
            output_name += ".pdf"
        output_path = os.path.join(args.output_dir, output_name)

        pdf_path = images_to_pdf(downloaded, output_path)
        if not pdf_path:
            sys.exit(1)

        # OCR
        if args.ocr:
            ocr_output = output_path.replace(".pdf", "_ocr.pdf")
            result = add_ocr_layer(pdf_path, ocr_output, lang=args.lang)
            if result and result != pdf_path:
                print(f"\nFinal OCR PDF: {ocr_output}")

        # Keep images if requested
        if args.keep_images:
            dest_dir = os.path.join(args.output_dir, f"{safe_title}_pages")
            os.makedirs(dest_dir, exist_ok=True)
            for src in downloaded:
                shutil.copy2(src, dest_dir)
            print(f"\nPage images saved to: {dest_dir}/")

    print(f"\nDone! Document saved to: {output_path}")
    print(f"View record: https://catalog.archives.gov/id/{naid}")


if __name__ == "__main__":
    main()
