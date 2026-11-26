# Debian 13 "trixie". Bookworm's regular security support ended in July 2026.
ARG DEBIAN_VERSION=13.7-slim

FROM debian:${DEBIAN_VERSION}

ARG RESTIC_VERSION=0.20.0
ARG RESTIC_SHA256=0ccd9fa7885074a24603b051e01bf9bd8b5e45d6a1791e9d9887e9e9372107f7
ARG RCLONE_VERSION=1.76.0
ARG RCLONE_SHA256=b00f00b3266c3764413afe599d3ec316f6f43b3e0ea8a0bc1d186d2133e46df7

ENV DEBIAN_FRONTEND=noninteractive

# zfsutils-linux lives in "contrib". apt packages are deliberately not version-pinned: Debian drops
# superseded security builds from its mirrors, so exact pins break the build after the next DSA.
# The base image tag above is what pins the release.
# --no-install-recommends keeps zfs-dkms (and a whole compiler toolchain) out of the image.
RUN sed -i 's/^Components: main$/Components: main contrib/' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl unzip bzip2 fuse3 zfsutils-linux python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/restic.bz2 \
        "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_amd64.bz2" && \
    echo "${RESTIC_SHA256}  /tmp/restic.bz2" | sha256sum -c - && \
    bzip2 -dc /tmp/restic.bz2 > /usr/local/bin/restic && \
    chmod 0755 /usr/local/bin/restic && \
    rm /tmp/restic.bz2 && \
    restic version

RUN curl -fsSL -o /tmp/rclone.zip \
        "https://github.com/rclone/rclone/releases/download/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-amd64.zip" && \
    echo "${RCLONE_SHA256}  /tmp/rclone.zip" | sha256sum -c - && \
    unzip -j -q /tmp/rclone.zip "rclone-v${RCLONE_VERSION}-linux-amd64/rclone" -d /usr/local/bin && \
    chmod 0755 /usr/local/bin/rclone && \
    rm /tmp/rclone.zip && \
    env -u RCLONE_VERSION -u RCLONE_SHA256 rclone version  # rclone reads RCLONE_* env vars as flags

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt && rm /tmp/requirements.txt

WORKDIR /app
COPY zfs_restic /app/zfs_restic

# Run history (SQLite). Mount a persistent host path / TrueNAS dataset here.
ENV DATA_DIR=/data \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
VOLUME /data

EXPOSE 8000
HEALTHCHECK --interval=60s --timeout=5s --start-period=15s CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1
CMD ["python3", "-m", "zfs_restic"]
