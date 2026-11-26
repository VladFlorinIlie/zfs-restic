ARG DEBIAN_VERSION=12.14-slim

FROM debian:${DEBIAN_VERSION}

ARG RESTIC_VERSION=0.19.0

ARG CURL_VERSION=7.88.1-10+deb12u14
ARG FUSE_VERSION=3.14.0-4
ARG ZFS_VERSION=2.1.11-1+deb12u1
ARG UNZIP_VERSION=6.0-28
ARG BZIP_VERSION=1.0.8-5+b1
ARG PYTHON_VERSION=3.11.2-1+b1
ARG PIP_VERSION=23.0.1+dfsg-1

RUN rm -f /etc/apt/sources.list.d/debian.sources && \
    echo "deb http://deb.debian.org/debian bookworm main contrib non-free-firmware" > /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bookworm-security main contrib non-free-firmware" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    curl=${CURL_VERSION} \
    fuse3=${FUSE_VERSION} \
    zfsutils-linux=${ZFS_VERSION} \
    unzip=${UNZIP_VERSION} \
    bzip2=${BZIP_VERSION} \
    python3=${PYTHON_VERSION} \
    python3-pip=${PIP_VERSION} \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o restic.bz2 https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_amd64.bz2 && \
    bzip2 -d restic.bz2 && \
    mv restic /usr/local/bin/restic && \
    chmod +x /usr/local/bin/restic

RUN curl -L https://rclone.org/install.sh | bash

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app.py backup.py db.py /app/
COPY static /app/static

WORKDIR /app

# Run history (SQLite). Mount a persistent host path / TrueNAS dataset here.
ENV DATA_DIR=/data PYTHONUNBUFFERED=1
VOLUME /data

EXPOSE 8000
HEALTHCHECK --interval=60s --timeout=5s --start-period=10s CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1
CMD ["python3", "app.py"]