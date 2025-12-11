FROM python:3.11-slim

# Set working directory
WORKDIR /usr/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/usr/app/dbt_project

# Default command
CMD ["tail", "-f", "/dev/null"]