FROM selenium/standalone-chrome:latest

USER root

# Update package lists and install necessary system packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    git \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get -yqq update && \
    apt-get -yqq install google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose a port (if needed)
EXPOSE 8080
EXPOSE 4444

# Command to keep the container running (replace with actual application command)
CMD ["bash"]
