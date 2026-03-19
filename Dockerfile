# Build natively on Apple Silicon: docker build --platform linux/arm64 -t nara-downloader .
FROM python:3.12-slim

# Install system dependencies for OCR
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-deu \
    ghostscript \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf2.0-0 \
    libffi-dev \
    libcairo2 \
    libjpeg62-turbo \
    libopenjp2-7 \
    libleptonica-dev \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir ocrmypdf pikepdf pdf2image pytesseract uvloop

COPY nara_download.py .

# Output directory (mount a volume here)
RUN mkdir /output
WORKDIR /output

ENTRYPOINT ["python", "/app/nara_download.py"]
CMD ["--help"]
