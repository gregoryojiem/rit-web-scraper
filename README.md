# Static Website Downloader

A Python script to download static websites, currently set up to download from the RIT site. 

It downloads all the publicly accessible sites from a base URL, accounting for nesting, and also saving resources (pdfs, docx, pngs, etc.)

Can save pages as HTML or Markdown.

## Requirements
- Python 3.11 (or a lower compatible version)
- Packages: 
  ```bash
  pip install requests beautifulsoup4 markdownify