FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fix permissions explicitly
RUN chmod 755 /app
RUN chmod 644 /app/main.py
RUN chmod 644 /app/requirements.txt

EXPOSE 5000

CMD ["python", "main.py"]
