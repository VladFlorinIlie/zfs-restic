FROM debian:bookworm-slim

RUN rm -f /etc/apt/sources.list.d/debian.sources && \
    echo "deb http://deb.debian.org/debian bookworm main contrib non-free-firmware" > /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bookworm-security main contrib non-free-firmware" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    curl \
    fuse3 \
    zfsutils-linux \
    unzip \
    bzip2 \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o restic.bz2 https://github.com/restic/restic/releases/download/v0.18.0/restic_0.18.0_linux_amd64.bz2 && \
    bzip2 -d restic.bz2 && \
    mv restic /usr/local/bin/restic && \
    chmod +x /usr/local/bin/restic

RUN curl -L https://rclone.org/install.sh | bash

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app.py /app/app.py
COPY backup.py /app/backup.py

WORKDIR /app

EXPOSE 8000
CMD ["python3", "app.py"]