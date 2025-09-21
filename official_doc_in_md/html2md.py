import os
import html2text
from pathlib import Path

def convert_html_to_markdown(site_dir):
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = False
    h.body_width = 0  # 行の折り返しを無効
    
    for html_file in Path(site_dir).rglob('*.html'):
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        markdown = h.handle(html_content)
        md_file = html_file.with_suffix('.md')
        
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(markdown)
        
        print(f"Converted: {html_file.name} -> {md_file.name}")

convert_html_to_markdown('.')
