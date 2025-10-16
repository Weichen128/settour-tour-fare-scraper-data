# 使用 Chainguard Python 開發映像
FROM asia-east1-docker.pkg.dev/testing-cola-rd/chainguard-images/python:latest-dev

# 設置環境變數
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    USE_POSIX_PATH=1

# 設置工作目錄
WORKDIR /app

# 複製 requirements.txt
COPY requirements.txt .

# 安裝依賴
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用代碼
COPY . .

# 創建日誌目錄並設置正確權限
# 使用 USER 0 臨時切換到 root 用戶來創建目錄
USER 0
RUN mkdir -p /app/logs && \
    chmod 777 /app/logs && \
    touch /app/logs/crawler.log && \
    chmod 666 /app/logs/crawler.log && \
    # 確保非 root 用戶擁有適當的權限
    chown -R nonroot:nonroot /app
# 切換回非 root 用戶
USER nonroot

# 暴露端口
EXPOSE 80

# 設置運行指令
ENTRYPOINT ["/bin/bash", "-c", "python southeast_travel_crawler/main.py"]
