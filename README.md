# Selenium Web Scraper Docker Setup

This repository contains a Docker setup for Python-based web scraping using Selenium with Chrome.

## Features

- Python 3.9 environment
- Selenium WebDriver with Chrome
- BeautifulSoup4, Pandas, and other useful libraries for data processing
- Volume mapping for easy development

## Requirements

- Docker
- Docker Compose

## Getting Started

1. Clone this repository
2. Build and run the Docker container:

```bash
docker-compose up --build
```

This will run the example web scraping script in `main.py`.

## Development

- The Docker container maps your local directory to `/app` in the container
- Any changes to your Python files will be reflected in the container
- Data is stored in the `./data` directory which is mapped to `/app/data` in the container

## Customizing

You can modify `main.py` to implement your own web scraping logic. The example script demonstrates:

- Setting up a headless Chrome browser
- Navigating to a website
- Waiting for elements to load
- Extracting data from the page

## Debugging

If you need to debug the container, uncomment the following lines in `docker-compose.yaml`:

```yaml
# command: bash
# stdin_open: true
# tty: true
```

Then run `docker-compose up --build` and connect to the container:

```bash
docker exec -it web_scraper bash
```

## Environment Variables

- `SELENIUM_HEADLESS`: Set to "false" if you want to run Chrome in non-headless mode (requires X server forwarding) 