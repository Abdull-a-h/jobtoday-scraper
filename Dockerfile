# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-unifont \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    libasound2 \
    libwayland-client0 \
    libxkbcommon0 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and browsers BEFORE copying application code
# This ensures consistent installation paths
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code AFTER installing Playwright
COPY . .

# Create directories for session and output files
RUN mkdir -p /app/data

# Expose port
EXPOSE 10000

# Set environment variables
ENV PORT=10000
ENV PLAYWRIGHT_SKIP_BROWSER_GC=1
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Run the application
CMD gunicorn --bind 0.0.0.0:$PORT \
    --timeout 1800 \
    --workers 1 \
    --threads 1 \
    --worker-tmp-dir /dev/shm \
    --log-level info \
    --access-logfile - \
    --error-logfile - \
    scraper_api:app