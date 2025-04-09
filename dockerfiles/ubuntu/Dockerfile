# Use an official lightweight Linux image
FROM ubuntu:latest

# Set environment variables to prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install necessary system packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    git \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside the container
# WORKDIR /app

# Create a virtual environment and activate it
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# RUN apt update
RUN apt-get install -y wget
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update && apt-get install -y google-chrome-stable


# Copy application files (if any)
# COPY . /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose a port (if needed)
EXPOSE 8080

# Command to keep the container running (replace with actual application command)
CMD ["bash"]
