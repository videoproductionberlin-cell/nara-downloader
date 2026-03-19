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
    "NARA-Downloader/3.1 "
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
    raise ValueError(f"Could not extract NAID from: {url_or_id}")


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
        self._semaphore: asyncio.Semaphore | None = None

    async def __aenter__(self) -> NaraClient:
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
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
        for attempt in range(1, MAX_RETRIES + 1):
            success = False
            async with self._semaphore:
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            with open(dest, "wb") as f:
                                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                                    f.write(chunk)
                            pbar.update(1)
                            success = True
                            # break out of semaphore block before pacing delay
                        elif resp.status == 429:
                            wait = RETRY_DELAY * attempt * 3
                            pbar.write(f"  Rate-limited on page {index+1}. Waiting {wait}s …")
                            await asyncio.sleep(wait)
                            continue
                        elif resp.status >= 500:
                            await asyncio.sleep(RETRY_DELAY * attempt)
                            continue
                        else:
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
            # Pacing delay outside semaphore to free the slot for others
            if success:
                if self.delay > 0:
                    await asyncio.sleep(self.delay)
                return (index, dest)

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
            parsed_path = urlparse(object_url).path
            ext = Path(parsed_path).suffix.lower() or ".jpg"
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
# PDF pipeline: prepare → compile (order-preserving)
# ---------------------------------------------------------------------------
def prepare_images(
    image_paths: list[str],
) -> list[tuple[str, str]]:
    """
    Normalize downloaded files into an ordered list of (kind, path) tuples.

    Each entry is either ("image", path) for img2pdf-compatible files,
    or ("pdf", path) for existing PDF pages. Original page order is
    preserved so that mixed image/PDF inputs are assembled correctly.

    Unknown formats are converted to JPEG. Invalid files are skipped.
    """
    prepared: list[tuple[str, str]] = []

    for path in image_paths:
        ext = Path(path).suffix.lower()
        if ext == ".pdf":
            prepared.append(("pdf", path))
        elif ext in IMG2PDF_EXTENSIONS:
            prepared.append(("image", path))
        elif ext in (".gif", ""):
            # Convert GIF / unknown to JPEG (img2pdf doesn't handle GIF)
            converted = path + ".jpg"
            try:
                img = Image.open(path)
                img.convert("RGB").save(converted, "JPEG", quality=95)
                prepared.append(("image", converted))
                img.close()
            except Exception as e:
                logger.warning("Could not convert %s: %s", path, e)
        else:
            # Unknown format — attempt Pillow conversion
            converted = path + ".jpg"
            try:
                img = Image.open(path)
                img.convert("RGB").save(converted, "JPEG", quality=95)
                prepared.append(("image", converted))
                img.close()
            except Exception as e:
                logger.warning("Could not process %s: %s", path, e)

    return prepared


def compile_pdf(
    prepared_pages: list[tuple[str, str]],
    output_path: str,
) -> str:
    """
    Assemble prepared pages into a single output PDF, preserving order.

    Strategy:
      1. Images only → img2pdf (lossless, fast, near-zero memory)
      2. Mixed images + PDFs → process runs of consecutive images via
         img2pdf, then merge all segments via pikepdf in original order
      3. Fallback → chunked Pillow if img2pdf fails
    """
    if not prepared_pages:
        logger.error("No valid files to combine.")
        return ""

    logger.info("Combining %d page(s) into PDF …", len(prepared_pages))

    has_pdfs = any(kind == "pdf" for kind, _ in prepared_pages)
    image_files = [p for kind, p in prepared_pages if kind == "image"]

    # Pure-image fast path
    if not has_pdfs:
        try:
            with open(output_path, "wb") as f:
                f.write(img2pdf.convert(image_files))
            _log_pdf_size(output_path)
            return output_path
        except Exception as e:
            logger.warning("img2pdf failed: %s — falling back to chunked Pillow", e)
            return _pillow_chunked_to_pdf(image_files, output_path)

    # Mixed path: preserve original order using runs
    return _merge_ordered_to_pdf(prepared_pages, output_path)


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


def _merge_ordered_to_pdf(
    prepared_pages: list[tuple[str, str]],
    output_path: str,
) -> str:
    """Merge images and PDFs into one output PDF preserving original page order.

    Groups consecutive images into runs, converts each run to a temp PDF
    via img2pdf, then merges all segments (image-run PDFs and existing PDFs)
    in the original order via pikepdf.
    """
    try:
        import pikepdf
    except ImportError:
        logger.warning(
            "pikepdf not installed — skipping embedded PDF pages. "
            "Install with: pip install pikepdf"
        )
        image_files = [p for kind, p in prepared_pages if kind == "image"]
        if image_files:
            return compile_pdf(
                [("image", f) for f in image_files], output_path
            )
        return ""

    # Build ordered list of PDF segments: each is a path to merge
    segments: list[str] = []
    temp_files: list[str] = []
    current_image_run: list[str] = []
    run_counter = 0

    def _flush_image_run() -> None:
        nonlocal run_counter
        if not current_image_run:
            return
        run_pdf = f"{output_path}.tmp_run_{run_counter:04d}.pdf"
        run_counter += 1
        try:
            with open(run_pdf, "wb") as f:
                f.write(img2pdf.convert(current_image_run))
            segments.append(run_pdf)
            temp_files.append(run_pdf)
        except Exception as e:
            logger.warning("img2pdf failed for image run: %s", e)
            fallback_pdf = f"{output_path}.tmp_run_{run_counter:04d}_fb.pdf"
            result = _pillow_chunked_to_pdf(current_image_run, fallback_pdf)
            if result:
                segments.append(result)
                temp_files.append(result)
        current_image_run.clear()

    for kind, path in prepared_pages:
        if kind == "image":
            current_image_run.append(path)
        else:
            # Flush any pending image run before this PDF
            _flush_image_run()
            segments.append(path)

    # Flush any trailing image run
    _flush_image_run()

    # Merge all segments in order
    merged = pikepdf.Pdf.new()
    for seg_path in segments:
        try:
            src = pikepdf.open(seg_path)
            merged.pages.extend(src.pages)
        except Exception as e:
            logger.warning("Could not merge %s: %s", seg_path, e)
    merged.save(output_path)
    merged.close()

    # Clean up temp files
    for path in temp_files:
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
        for i, img in enumerate(tqdm(_pdf_to_images(input_pdf), desc="OCR processing", unit="page")):
            text = pytesseract.image_to_string(img, lang=lang)
            full_text.append(f"--- Page {i+1} ---\n{text}")
            img.close()

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


def _pdf_to_images(pdf_path: str):
    """Yield PDF pages one at a time as PIL Images for OCR processing.

    Streams page-by-page to avoid loading the entire document into RAM.
    A 1,000-page document at 300 DPI would consume ~20-30 GB if loaded
    at once; this generator keeps memory usage bounded to one page.
    """
    try:
        from pdf2image import convert_from_path
        from pdf2image.pdf2image import pdfinfo_from_path
    except ImportError:
        logger.warning("pdf2image not installed — cannot convert PDF to images for OCR")
        return

    try:
        info = pdfinfo_from_path(pdf_path)
        max_pages = info["Pages"]
    except Exception as e:
        logger.warning("Could not read PDF info: %s — falling back to full load", e)
        yield from convert_from_path(pdf_path, dpi=300)
        return

    for page_num in range(1, max_pages + 1):
        images = convert_from_path(
            pdf_path, dpi=300, first_page=page_num, last_page=page_num
        )
        if images:
            yield images[0]


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
    logger.info(" NARA Catalog Document Downloader v3.1")
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
            raise RuntimeError(f"No record found for NAID {naid}")

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
            raise RuntimeError(
                "No downloadable digital objects found for this record. "
                "The record may not have been digitized, or may use a "
                "different format (e.g. electronic records/data files)."
            )

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
                raise RuntimeError("No pages were downloaded successfully.")

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

            prepared_pages = prepare_images(downloaded)
            pdf_path = compile_pdf(prepared_pages, output_path)
            if not pdf_path:
                raise RuntimeError("PDF compilation failed — no valid files to combine.")

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
    try:
        asyncio.run(async_main(args))
    except (ValueError, RuntimeError) as e:
        logger.error("%s", e)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
