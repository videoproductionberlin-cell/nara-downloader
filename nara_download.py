#!/usr/bin/env python3
"""
NARA Catalog Document Downloader
=================================
Downloads all pages of a document from the National Archives Catalog
(catalog.archives.gov) and combines them into a single PDF with
optional OCR (searchable text layer).

Optimized for large (1,000+ page) documents:
  - Fully async via aiohttp (single network stack, no threads)
  - Lossless PDF creation via img2pdf (zero decompression)
  - Chunked Pillow fallback to prevent OOM on huge documents
  - Optional uvloop for faster event loop on macOS/Linux

Can be used as a CLI tool or imported as a library:

    from nara_download import NaraClient

    async with NaraClient() as client:
        record = await client.fetch_record("595500")
        objects = client.get_digital_objects(record)
        paths = await client.download_pages(objects, "/tmp/pages")

Usage (CLI):
    python nara_download.py <URL or NAID> [options]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import img2pdf
from PIL import Image
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Optional: use uvloop for faster async I/O (drop-in replacement)
# ---------------------------------------------------------------------------
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("nara_downloader")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROXY_SEARCH = "https://catalog.archives.gov/proxy/v3/records/search"
PROXY_SEARCH_V2 = "https://catalog.archives.gov/proxy/records/search"
CHILD_RECORDS = "https://catalog.archives.gov/proxy/records/parentNaId"

USER_AGENT = (
    "NARA-Downloader/3.0 "
    "(Document research tool; contact: github.com/nara-downloader)"
)

# Retry logic
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Async download defaults
DEFAULT_MAX_CONCURRENT = 20
CHUNK_SIZE = 64 * 1024  # 64 KB read chunks for async downloads
PILLOW_CHUNK_SIZE = 50  # pages per chunk in Pillow fallback

# Supported image extensions for img2pdf (lossless path)
IMG2PDF_EXTENSIONS = frozenset((".jpg", ".jpeg", ".png", ".tif", ".tiff"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_naid(url_or_id: str) -> str:
    """Extract the NAID from a URL or plain number."""
    url_or_id = url_or_id.strip().rstrip("/")
    if url_or_id.isdigit():
        return url_or_id
    m = re.search(r"/id/(\d+)", url_or_id)
    if m:
        return m.group(1)
    parts = urlparse(url_or_id).path.strip("/").split("/")
    for part in reversed(parts):
        if part.isdigit():
            return part
    logger.error("Could not extract NAID from: %s", url_or_id)
    sys.exit(1)


def sanitize_filename(title: str, max_length: int = 100) -> str:
    """
    Create a safe filename from a record title.
    Truncates at the nearest word boundary (underscore) to avoid
    cutting words in half.
    """
    safe = re.sub(r'[<>:"/\\|?*]', "", title)
    safe = re.sub(r"\s+", "_", safe.strip())
    if len(safe) > max_length:
        # Find the last underscore before the limit
        truncated = safe[:max_length]
        last_break = truncated.rfind("_")
        if last_break > max_length // 2:
            safe = truncated[:last_break]
        else:
            safe = truncated
    return safe or "nara_document"


def _extract_hits(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract hit records from the NARA API response."""
    if not data:
        return []
    body = data.get("body", data)
    hits_obj = body.get("hits", {})
    hits = hits_obj.get("hits", [])
    records: list[dict[str, Any]] = []
    for hit in hits:
        source = hit.get("_source", hit)
        record = source.get("record", source)
        if record:
            records.append(record)
    return records


def get_digital_objects(record: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract digital objects (pages) from a record, sorted by page number."""
    objects: list[dict[str, Any]] = record.get("digitalObjects", [])
    if not objects:
        objects = record.get("objects", [])
    if not objects:
        return []

    def sort_key(obj: dict[str, Any]) -> int:
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


# ---------------------------------------------------------------------------
# NaraClient — encapsulates all network I/O
# ---------------------------------------------------------------------------
class NaraClient:
    """
    Async client for the NARA Catalog API.

    Usage:
        async with NaraClient() as client:
            record = await client.fetch_record("595500")
            objects = get_digital_objects(record)
            paths = await client.download_pages(objects, "/tmp/pages")
    """

    def __init__(
        self,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        delay: float = 0.1,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.delay = delay
        self._session: aiohttp.ClientSession | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def __aenter__(self) -> NaraClient:
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=120, sock_read=60)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("NaraClient must be used as an async context manager")
        return self._session

    # --- API requests (JSON) -----------------------------------------------

    async def _api_get(
        self, url: str, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """GET JSON with retry logic and rate-limit backoff."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    if resp.status == 429:
                        wait = RETRY_DELAY * attempt * 2
                        logger.warning("Rate-limited. Waiting %ds …", wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        logger.warning(
                            "Server error (%d). Retry %d/%d …",
                            resp.status, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(RETRY_DELAY * attempt)
                        continue
                    body = await resp.text()
                    logger.error("HTTP %d for %s — %s", resp.status, url, body[:500])
                    return {}
            except Exception as e:
                logger.warning("Connection error: %s. Retry %d/%d …", e, attempt, MAX_RETRIES)
                await asyncio.sleep(RETRY_DELAY * attempt)
        logger.error("Failed after %d attempts: %s", MAX_RETRIES, url)
        return {}

    async def fetch_record(self, naid: str) -> dict[str, Any]:
        """Fetch a record by NAID. Returns the record dict or {}."""
        logger.info("Fetching record metadata for NAID %s …", naid)

        data = await self._api_get(PROXY_SEARCH, params={
            "naId_is": naid,
            "allowLegacyOrgNames": "true",
            "includeExtractedText": "true",
            "includeOtherExtractedText": "true",
        })
        hits = _extract_hits(data)
        if hits:
            return hits[0]

        logger.info("v3 endpoint returned no results, trying v2 …")
        data = await self._api_get(PROXY_SEARCH_V2, params={"naId": naid})
        hits = _extract_hits(data)
        if hits:
            return hits[0]

        logger.error("No record found for NAID %s", naid)
        return {}

    async def fetch_child_file_units(self, naid: str) -> list[dict[str, Any]]:
        """Fetch child file units for a series/record group."""
        logger.info("Checking for child records under NAID %s …", naid)
        all_children: list[dict[str, Any]] = []
        page = 1
        while True:
            data = await self._api_get(f"{CHILD_RECORDS}/{naid}", params={
                "abbreviated": "true",
                "limit": "50",
                "sort": "naId:asc",
                "page": str(page),
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

    # --- Image downloads ---------------------------------------------------

    async def _download_one(
        self,
        url: str,
        dest: str,
        index: int,
        pbar: tqdm,
    ) -> tuple[int, str | None]:
        """Download a single file. Returns (index, path) or (index, None)."""
        async with self._semaphore:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            with open(dest, "wb") as f:
                                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                                    f.write(chunk)
                            pbar.update(1)
                            if self.delay > 0:
                                await asyncio.sleep(self.delay)
                            return (index, dest)
                        if resp.status == 429:
                            wait = RETRY_DELAY * attempt * 3
                            pbar.write(f"  Rate-limited on page {index+1}. Waiting {wait}s …")
                            await asyncio.sleep(wait)
                            continue
                        if resp.status >= 500:
                            await asyncio.sleep(RETRY_DELAY * attempt)
                            continue
                        pbar.write(
                            f"  HTTP {resp.status} downloading page {index+1}. "
                            f"Retry {attempt}/{MAX_RETRIES} …"
                        )
                        await asyncio.sleep(RETRY_DELAY)
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        pbar.write(
                            f"  Download error page {index+1}: {e}. "
                            f"Retry {attempt}/{MAX_RETRIES} …"
                        )
                        await asyncio.sleep(RETRY_DELAY * attempt)
                    else:
                        pbar.write(f"  [WARN] Failed to download page {index+1}: {e}")

            pbar.update(1)
            return (index, None)

    async def download_pages(
        self,
        objects: list[dict[str, Any]],
        tmp_dir: str,
    ) -> list[str]:
        """
        Download all digital object images to tmp_dir.
        Returns list of local file paths in page order.
        """
        if not objects:
            logger.error("No digital objects found for this record.")
            return []

        loop_info = "uvloop" if _UVLOOP else "asyncio"
        logger.info(
            "Found %d page(s). Downloading (%d concurrent, %s) …",
            len(objects), self.max_concurrent, loop_info,
        )

        tasks: list[asyncio.Task[tuple[int, str | None]]] = []
        pbar = tqdm(total=len(objects), desc="Downloading pages", unit="page")

        for i, obj in enumerate(objects):
            object_url = obj.get("objectUrl", "")
            if not object_url:
                pbar.update(1)
                continue

            page_num = obj.get("pageNum", i + 1)
            ext = Path(object_url).suffix.lower() or ".jpg"
            local_path = os.path.join(tmp_dir, f"page_{page_num:05d}{ext}")

            task = asyncio.create_task(
                self._download_one(object_url, local_path, i, pbar)
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        pbar.close()

        downloaded: dict[int, str] = {}
        for index, path in results:
            if path:
                downloaded[index] = path

        return [downloaded[i] for i in sorted(downloaded)]


# ---------------------------------------------------------------------------
# PDF pipeline: prepare → compile
# ---------------------------------------------------------------------------
def prepare_images(image_paths: list[str]) -> tuple[list[str], list[str]]:
    """
    Normalize downloaded files into two clean lists:
    - image_files: paths suitable for img2pdf (JPEG, PNG, TIFF)
    - pdf_files: existing PDF pages that need pikepdf merging

    Unknown formats are converted to JPEG. Invalid files are skipped.
    """
    image_files: list[str] = []
    pdf_files: list[str] = []

    for path in image_paths:
        ext = Path(path).suffix.lower()
        if ext == ".pdf":
            pdf_files.append(path)
        elif ext in IMG2PDF_EXTENSIONS:
            image_files.append(path)
        elif ext == ".gif":
            # Convert GIF to JPEG (img2pdf doesn't handle GIF)
            converted = path + ".jpg"
            try:
                img = Image.open(path)
                img.convert("RGB").save(converted, "JPEG", quality=95)
                image_files.append(converted)
                img.close()
            except Exception as e:
                logger.warning("Could not convert %s: %s", path, e)
        else:
            # Unknown format — attempt Pillow conversion
            converted = path + ".jpg"
            try:
                img = Image.open(path)
                img.convert("RGB").save(converted, "JPEG", quality=95)
                image_files.append(converted)
                img.close()
            except Exception as e:
                logger.warning("Could not process %s: %s", path, e)

    return image_files, pdf_files


def compile_pdf(
    image_files: list[str],
    pdf_files: list[str],
    output_path: str,
) -> str:
    """
    Assemble prepared image and PDF files into a single output PDF.

    Strategy:
      1. Images only → img2pdf (lossless, fast, near-zero memory)
      2. Mixed images + PDFs → img2pdf for images, pikepdf to merge all
      3. Fallback → chunked Pillow if img2pdf fails
    """
    total = len(image_files) + len(pdf_files)
    if total == 0:
        logger.error("No valid files to combine.")
        return ""

    logger.info("Combining %d page(s) into PDF …", total)

    # Pure-image fast path
    if not pdf_files:
        try:
            with open(output_path, "wb") as f:
                f.write(img2pdf.convert(image_files))
            _log_pdf_size(output_path)
            return output_path
        except Exception as e:
            logger.warning("img2pdf failed: %s — falling back to chunked Pillow", e)
            return _pillow_chunked_to_pdf(image_files, output_path)

    # Mixed path: images + existing PDFs → merge via pikepdf
    return _merge_mixed_to_pdf(image_files, pdf_files, output_path)


def _log_pdf_size(path: str) -> None:
    size_mb = os.path.getsize(path) / (1024 * 1024)
    logger.info("PDF saved: %s (%.1f MB)", path, size_mb)


def _pillow_chunked_to_pdf(image_paths: list[str], output_path: str) -> str:
    """
    Fallback: create PDF via Pillow in chunks, then merge with pikepdf.
    Prevents OOM by never holding more than PILLOW_CHUNK_SIZE images in RAM.
    """
    if not image_paths:
        return ""

    chunk_count = (len(image_paths) + PILLOW_CHUNK_SIZE - 1) // PILLOW_CHUNK_SIZE

    if chunk_count == 1:
        return _pillow_single_chunk_to_pdf(image_paths, output_path)

    logger.info("Processing in %d chunks of up to %d pages …", chunk_count, PILLOW_CHUNK_SIZE)
    chunk_paths: list[str] = []
    tmp_dir = os.path.dirname(output_path) or "."

    for chunk_idx in range(chunk_count):
        start = chunk_idx * PILLOW_CHUNK_SIZE
        end = min(start + PILLOW_CHUNK_SIZE, len(image_paths))
        chunk = image_paths[start:end]
        chunk_path = os.path.join(tmp_dir, f".nara_chunk_{chunk_idx:04d}.pdf")

        result = _pillow_single_chunk_to_pdf(chunk, chunk_path)
        if result:
            chunk_paths.append(result)
        logger.debug("Chunk %d/%d done (%d pages)", chunk_idx + 1, chunk_count, end - start)

    if not chunk_paths:
        return ""

    if len(chunk_paths) == 1:
        shutil.move(chunk_paths[0], output_path)
    else:
        try:
            import pikepdf
            merged = pikepdf.Pdf.new()
            for cp in chunk_paths:
                src = pikepdf.open(cp)
                merged.pages.extend(src.pages)
            merged.save(output_path)
            merged.close()
        except ImportError:
            logger.warning("pikepdf not installed — only first chunk saved.")
            shutil.move(chunk_paths[0], output_path)

    for cp in chunk_paths:
        try:
            os.remove(cp)
        except OSError:
            pass

    _log_pdf_size(output_path)
    return output_path


def _pillow_single_chunk_to_pdf(image_paths: list[str], output_path: str) -> str:
    """Convert a small batch of images to PDF via Pillow. Frees memory after save."""
    if not image_paths:
        return ""

    images: list[Image.Image] = []
    for path in image_paths:
        try:
            img = Image.open(path)
            if img.mode != "RGB":
                img = img.convert("RGB")
            images.append(img)
        except Exception as e:
            logger.warning("Could not process %s: %s", path, e)

    if not images:
        return ""

    first = images[0]
    rest = images[1:]
    first.save(output_path, "PDF", save_all=True, append_images=rest, resolution=300)

    for img in images:
        img.close()
    del images

    return output_path


def _merge_mixed_to_pdf(
    image_files: list[str],
    pdf_files: list[str],
    output_path: str,
) -> str:
    """Merge images and existing PDFs into one output PDF via pikepdf."""
    try:
        import pikepdf
    except ImportError:
        logger.warning("pikepdf not installed — skipping embedded PDF pages. Install with: pip install pikepdf")
        if image_files:
            return compile_pdf(image_files, [], output_path)
        return ""

    temp_pdfs: list[str] = []

    if image_files:
        images_pdf = output_path + ".tmp_images.pdf"
        try:
            with open(images_pdf, "wb") as f:
                f.write(img2pdf.convert(image_files))
            temp_pdfs.append(images_pdf)
        except Exception as e:
            logger.warning("img2pdf failed for images: %s", e)
            images_pdf_fallback = output_path + ".tmp_images_fb.pdf"
            result = _pillow_chunked_to_pdf(image_files, images_pdf_fallback)
            if result:
                temp_pdfs.append(result)

    temp_pdfs.extend(pdf_files)

    merged = pikepdf.Pdf.new()
    for pdf_path in temp_pdfs:
        try:
            src = pikepdf.open(pdf_path)
            merged.pages.extend(src.pages)
        except Exception as e:
            logger.warning("Could not merge %s: %s", pdf_path, e)
    merged.save(output_path)
    merged.close()

    for path in temp_pdfs:
        if ".tmp_" in path:
            try:
                os.remove(path)
            except OSError:
                pass

    _log_pdf_size(output_path)
    return output_path


# ---------------------------------------------------------------------------
# OCR
# ---------------------------------------------------------------------------
def add_ocr_layer(input_pdf: str, output_pdf: str, lang: str = "eng") -> str:
    """
    Add a searchable text layer to the PDF using ocrmypdf.
    Falls back to pytesseract text extraction if ocrmypdf is unavailable.
    """
    logger.info("Adding OCR text layer (language: %s) …", lang)

    try:
        import ocrmypdf

        has_unpaper = shutil.which("unpaper") is not None
        has_gs = shutil.which("gs") is not None

        kwargs: dict[str, Any] = dict(
            language=lang,
            deskew=True,
            optimize=1 if has_gs else 0,
            skip_text=True,
            progress_bar=True,
        )
        if has_unpaper:
            kwargs["clean"] = True

        ocrmypdf.ocr(input_pdf, output_pdf, **kwargs)
        size_mb = os.path.getsize(output_pdf) / (1024 * 1024)
        logger.info("OCR PDF saved: %s (%.1f MB)", output_pdf, size_mb)
        return output_pdf
    except ImportError:
        pass
    except Exception as e:
        logger.warning("ocrmypdf failed: %s", e)

    try:
        import pytesseract

        logger.info("Using pytesseract fallback (text extraction only) …")
        full_text: list[str] = []
        pdf_images = _pdf_to_images(input_pdf)
        for i, img in enumerate(tqdm(pdf_images, desc="OCR processing", unit="page")):
            text = pytesseract.image_to_string(img, lang=lang)
            full_text.append(f"--- Page {i+1} ---\n{text}")

        text_path = input_pdf.replace(".pdf", "_ocr_text.txt")
        with open(text_path, "w", encoding="utf-8") as f:
            f.write("\n\n".join(full_text))
        logger.info("OCR text saved: %s", text_path)

        shutil.copy2(input_pdf, output_pdf)
        logger.info("PDF copied (text layer requires ocrmypdf): %s", output_pdf)
        return output_pdf

    except ImportError:
        logger.error(
            "Neither ocrmypdf nor pytesseract is available. "
            "Install one of: pip install ocrmypdf  |  pip install pytesseract"
        )
        return input_pdf


def _pdf_to_images(pdf_path: str) -> list[Image.Image]:
    """Convert PDF pages to PIL Images for OCR processing."""
    try:
        from pdf2image import convert_from_path
        return convert_from_path(pdf_path, dpi=300)
    except ImportError:
        logger.warning("pdf2image not installed — cannot convert PDF to images for OCR")
        return []


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------
def extract_nara_text(
    record: dict[str, Any], objects: list[dict[str, Any]]
) -> str:
    """Extract any OCR text that NARA already has for this record."""
    texts: list[str] = []
    for obj in objects:
        text = obj.get("extractedText", "")
        if text:
            page = obj.get("pageNum", "?")
            texts.append(f"--- Page {page} ---\n{text}")
    return "\n\n".join(texts)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def _configure_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Set up logging based on user preference."""
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(level)


async def async_main(args: argparse.Namespace) -> None:
    """Async entry point — runs the full download pipeline."""
    naid = extract_naid(args.target)
    logger.info("=" * 60)
    logger.info(" NARA Catalog Document Downloader v3.0")
    logger.info(" NAID: %s", naid)
    logger.info(" URL:  https://catalog.archives.gov/id/%s", naid)
    if _UVLOOP:
        logger.info(" Loop: uvloop (accelerated)")
    logger.info("=" * 60 + "\n")

    async with NaraClient(
        max_concurrent=args.concurrent,
        delay=args.delay,
    ) as client:
        # Fetch record
        record = await client.fetch_record(naid)
        if not record:
            sys.exit(1)

        title = record.get("title", f"NAID_{naid}")
        level = record.get("levelOfDescription", "unknown")
        logger.info("Title: %s", title)
        logger.info("Level: %s\n", level)

        # Get digital objects
        objects = get_digital_objects(record)

        if not objects and level in ("series", "recordGroup", "collection"):
            logger.info("This is a container record (series/collection).")
            children = await client.fetch_child_file_units(naid)
            if children:
                logger.info("Found %d child record(s).", len(children))
                logger.info(
                    "Downloading the first child record. "
                    "For bulk downloads, run this tool once per child NAID.\n"
                )
                for child in children[:5]:
                    child_naid = child.get("naId", "")
                    child_title = child.get("title", "untitled")
                    logger.info("  - NAID %s: %s", child_naid, child_title)
                if len(children) > 5:
                    logger.info("  … and %d more", len(children) - 5)

                first_child = children[0]
                child_objects = get_digital_objects(first_child)
                if child_objects:
                    objects = child_objects
                    title = first_child.get("title", title)
                    naid = str(first_child.get("naId", naid))
                    logger.info("Downloading: %s (NAID %s)\n", title, naid)
                else:
                    child_naid_str = str(first_child.get("naId", ""))
                    if child_naid_str:
                        child_record = await client.fetch_record(child_naid_str)
                        if child_record:
                            child_objects = get_digital_objects(child_record)
                            if child_objects:
                                objects = child_objects
                                title = child_record.get("title", title)
                                naid = child_naid_str

        if not objects:
            logger.error(
                "No downloadable digital objects found for this record.\n"
                "The record may not have been digitized, or may use a "
                "different format (e.g. electronic records/data files)."
            )
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
                logger.info("NARA OCR text saved: %s\n", text_path)
            else:
                logger.info("No existing OCR text found in NARA metadata.\n")

        # Prepare output directory
        os.makedirs(args.output_dir, exist_ok=True)
        safe_title = sanitize_filename(title)

        # Download
        with tempfile.TemporaryDirectory(prefix="nara_") as tmp_dir:
            downloaded = await client.download_pages(objects, tmp_dir)

            if not downloaded:
                logger.error("No pages were downloaded successfully.")
                sys.exit(1)

            logger.info(
                "\nSuccessfully downloaded %d/%d page(s).",
                len(downloaded), len(objects),
            )

            # Images-only mode
            if args.images_only:
                dest_dir = os.path.join(args.output_dir, f"{safe_title}_pages")
                os.makedirs(dest_dir, exist_ok=True)
                for src in downloaded:
                    shutil.copy2(src, dest_dir)
                logger.info("\nImages saved to: %s/", dest_dir)
                return

            # Prepare → Compile PDF
            output_name = args.output or f"{safe_title}.pdf"
            if not output_name.lower().endswith(".pdf"):
                output_name += ".pdf"
            output_path = os.path.join(args.output_dir, output_name)

            image_files, pdf_files = prepare_images(downloaded)
            pdf_path = compile_pdf(image_files, pdf_files, output_path)
            if not pdf_path:
                sys.exit(1)

            # OCR
            if args.ocr:
                ocr_output = output_path.replace(".pdf", "_ocr.pdf")
                result = add_ocr_layer(pdf_path, ocr_output, lang=args.lang)
                if result and result != pdf_path:
                    logger.info("\nFinal OCR PDF: %s", ocr_output)

            # Keep images if requested
            if args.keep_images:
                dest_dir = os.path.join(args.output_dir, f"{safe_title}_pages")
                os.makedirs(dest_dir, exist_ok=True)
                for src in downloaded:
                    shutil.copy2(src, dest_dir)
                logger.info("\nPage images saved to: %s/", dest_dir)

        logger.info("\nDone! Document saved to: %s", output_path)
        logger.info("View record: https://catalog.archives.gov/id/%s", naid)


def main() -> None:
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
    parser.add_argument("target", help="NARA Catalog URL or NAID number")
    parser.add_argument("-o", "--output", help="Output PDF filename (default: auto-generated from title)")
    parser.add_argument("--ocr", action="store_true", help="Add OCR text layer to the PDF (requires tesseract)")
    parser.add_argument("--lang", default="eng", help="OCR language(s), e.g. 'eng', 'deu', 'deu+eng' (default: eng)")
    parser.add_argument("--keep-images", action="store_true", help="Keep downloaded page images after creating PDF")
    parser.add_argument("--images-only", action="store_true", help="Download individual page images without creating a PDF")
    parser.add_argument("--output-dir", default=".", help="Output directory (default: current directory)")
    parser.add_argument("--extract-text", action="store_true", help="Extract NARA's existing OCR text to a .txt file")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between downloads in seconds (default: 0.1)")
    parser.add_argument("--concurrent", type=int, default=DEFAULT_MAX_CONCURRENT, help=f"Max concurrent downloads (default: {DEFAULT_MAX_CONCURRENT})")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show debug output")
    parser.add_argument("-q", "--quiet", action="store_true", help="Only show warnings and errors")

    args = parser.parse_args()
    _configure_logging(verbose=args.verbose, quiet=args.quiet)
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
