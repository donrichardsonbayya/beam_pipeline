FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2 and timezones
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev tzdata && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Make Beam use a stable polling strategy
ENV GRPC_POLL_STRATEGY=poll
ENV PYTHONUNBUFFERED=1

# Default command runs Beam locally in-memory with one worker
CMD ["python","-u","app/main.py","--runner=DirectRunner","--direct_running_mode=in_memory","--direct_num_workers=1"]