FROM python:3.12-slim
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Data CSVs are pre-generated and included in mcp_server/data/
# (no network downloads needed at build time)

# Cloud Run uses PORT env var (default 8080)
ENV PORT=8080
ENV MCP_PORT=8001

EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --ws-ping-interval 60 --ws-ping-timeout 60"]
