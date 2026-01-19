# 🐍 Base image with Python 3.11
FROM python:3.11-slim

# 👇 Install system and FFmpeg build dependencies + mediainfo
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git build-essential pkg-config yasm nasm cmake \
        libaom-dev libx264-dev libx265-dev libass-dev libfreetype6-dev libopus-dev libvpx-dev \
        libnuma-dev python3-dev wget curl aria2 p7zip-full mediainfo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 🧩 Build SVT-AV1 from source (lightweight)
RUN git clone --depth=1 https://gitlab.com/AOMediaCodec/SVT-AV1.git /tmp/SVT-AV1 && \
    cd /tmp/SVT-AV1/Build && cmake .. && make -j$(nproc) && make install && \
    rm -rf /tmp/SVT-AV1

# ⚙️ Update library paths so FFmpeg finds libSvtAv1Enc.so.3
RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/local-lib.conf && ldconfig

# 🎬 Build FFmpeg with SVT-AV1 + all required codecs
RUN git clone --depth=1 https://github.com/FFmpeg/FFmpeg.git /tmp/ffmpeg && \
    cd /tmp/ffmpeg && \
    ./configure \
        --prefix=/usr/local \
        --pkg-config-flags="--static" \
        --extra-cflags="-I/usr/local/include" \
        --extra-ldflags="-L/usr/local/lib" \
        --extra-libs="-lpthread -lm" \
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
    make -j$(nproc) && make install && \
    rm -rf /tmp/ffmpeg

# ✅ Verify FFmpeg build and encoder support
RUN ffmpeg -hide_banner -encoders | grep -E 'svt|aom|x26|opus|vpx|ass' && \
    mediainfo --version

# 🧰 Set working directory
WORKDIR /app

# 📦 Copy project files
COPY . .

# 🐍 Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 🔥 Expose Aria2 RPC port if used
EXPOSE 6800

# 🚀 Run your bot
CMD ["python3", "bot.py"]
