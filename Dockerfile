FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev \
    git build-essential pkg-config yasm nasm cmake \
    libaom-dev libx264-dev libx265-dev libass-dev \
    libfreetype6-dev libopus-dev libvpx-dev \
    libnuma-dev wget curl aria2 p7zip-full mediainfo \
    ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Build SVT-AV1
RUN git clone --depth=1 https://gitlab.com/AOMediaCodec/SVT-AV1.git /tmp/SVT-AV1 && \
    cd /tmp/SVT-AV1/Build && \
    cmake .. && \
    make -j6 && make install && \
    rm -rf /tmp/SVT-AV1

# Linker config
RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/local-lib.conf && ldconfig

# Build FFmpeg
RUN git clone --depth=1 https://github.com/FFmpeg/FFmpeg.git /tmp/ffmpeg && \
    cd /tmp/ffmpeg && \
    ./configure \
      --prefix=/usr/local \
      --bindir=/usr/local/bin \
      --enable-gpl \
      --enable-nonfree \
      --enable-libsvtav1 \
      --enable-libaom \
      --enable-libx264 \
      --enable-libx265 \
      --enable-libopus \
      --enable-libvpx \
      --enable-libass \
      --enable-libfreetype && \
    make -j6 && make install && \
    rm -rf /tmp/ffmpeg

# Verify
RUN ffmpeg -hide_banner -encoders | grep -E "svt|aom|x26|opus|vpx|ass" && mediainfo --version

WORKDIR /app
COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8000
CMD ["python3", "ravi.py"]



