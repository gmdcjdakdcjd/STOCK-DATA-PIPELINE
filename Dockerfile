# Python 배치 전용 이미지
FROM python:3.10-slim

ENV TZ=Asia/Seoul

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 이거 추가
COPY . .

CMD ["sleep", "infinity"]
