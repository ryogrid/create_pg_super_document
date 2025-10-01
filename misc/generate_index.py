#!/usr/bin/env python3
"""
Generated Documentation Index Generator

This script scans the generated_docs directory structure and
generates an accurate table of contents (index.md).
"""

import os
import re
from collections import defaultdict

def extract_markdown_files(docs_dir):
    """
    Collect .md files from the generated_docs directory and
    group them by directory
    """
    files_by_dir = defaultdict(list)
    
    # Scan the generated_docs directory
    for root, dirs, files in os.walk(docs_dir):
        # Get relative path from generated_docs
        rel_path = os.path.relpath(root, docs_dir)
        
        # Skip directories (jekyll related)
        if rel_path.startswith('_') or rel_path == '.':
            continue
            
        # Collect only .md files (excluding index.md)
        md_files = [f for f in files if f.endswith('.md') and f != 'index.md']
        
        if md_files:
            # Get directory name (process only single-character directories)
            dir_name = os.path.basename(root)
            if len(dir_name) == 1 and dir_name.isalpha():
                files_by_dir[dir_name].extend(md_files)
    
    return files_by_dir

def get_file_title(file_path):
    """
    Get display title from .md file
    (remove .md extension)
    """
    return os.path.splitext(file_path)[0]

def generate_navigation_links(files_by_dir):
    """
    Generate A-Z navigation links based on existing directories
    Split into uppercase and lowercase sections
    """
    uppercase_links = []
    lowercase_links = []
    
    for char in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        # Check uppercase directory
        if char in files_by_dir:
            uppercase_links.append(f'[{char}](#{char.lower()})')
        else:
            uppercase_links.append(char)  # No link for empty sections
            
        # Check lowercase directory
        if char.lower() in files_by_dir:
            lowercase_links.append(f'[{char.lower()}](#{char.lower()}-lowercase)')
        else:
            lowercase_links.append(char.lower())  # No link for empty sections
    
    # Split into two rows for each case
    uppercase_first_row = ' | '.join(uppercase_links[:12])
    uppercase_second_row = ' | '.join(uppercase_links[12:])
    
    lowercase_first_row = ' | '.join(lowercase_links[:12])
    lowercase_second_row = ' | '.join(lowercase_links[12:])
    
    result = "**Uppercase symbols:**\n"
    result += f"{uppercase_first_row}\n{uppercase_second_row}\n\n"
    result += "**Lowercase symbols:**\n"
    result += f"{lowercase_first_row}\n{lowercase_second_row}"
    
    return result

def generate_directory_section(dir_name, files, is_lowercase=False):
    """
    Generate table of contents section for a specific directory
    """
    if not files:
        return ""
    
    # Sort files (dictionary order)
    sorted_files = sorted(files, key=lambda x: x.lower())
    
    # Extract titles
    file_titles = [get_file_title(f) for f in sorted_files]
    
    # Generate section header with appropriate anchor
    if is_lowercase:
        section = f'\n<a id="{dir_name.lower()}-lowercase"></a>\n## {dir_name.lower()} (lowercase)\n\n'
    else:
        section = f"\n## {dir_name.upper()}\n\n"
    
    section += "| Name | Name | Name | Name | Name | Name |\n"
    section += "| --- | --- | --- | --- | --- | --- |\n"
    
    # Group by 6
    for i in range(0, len(file_titles), 6):
        row_titles = file_titles[i:i+6]
        row_files = sorted_files[i:i+6]
        
        # Generate markdown links
        links = []
        for title, filename in zip(row_titles, row_files):
            link = f"[{title}]({dir_name}/{filename})"
            links.append(link)
        
        # Fill with empty strings if less than 6 columns
        while len(links) < 6:
            links.append("")
        
        section += "| " + " | ".join(links) + " |\n"
    
    return section

def generate_index_content(files_by_dir):
    """
    Generate complete index.md content
    """
    content = """# Generated Documentation Index

Quick jump by directory:
"""
    
    # Add navigation links
    content += generate_navigation_links(files_by_dir)
    content += "\n\n"
    
    content += """
This index lists all generated markdown documents grouped by directory. Uppercase and lowercase directories are listed separately. Within each section, entries are sorted in dictionary order and laid out in a 6-column table.
"""
    
    # Process directories in alphabetical order
    for char in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        # First process uppercase directory
        if char in files_by_dir:
            content += generate_directory_section(char, files_by_dir[char], is_lowercase=False)
        
        # Then process lowercase directory
        if char.lower() in files_by_dir:
            content += generate_directory_section(char.lower(), files_by_dir[char.lower()], is_lowercase=True)
    
    return content

def main():
    """Main processing"""
    docs_dir = '/home/ryo/work/postgres_17_6_sub/generated_docs'
    output_file = os.path.join(docs_dir, 'index.md')
    
    if not os.path.exists(docs_dir):
        print(f"Error: {docs_dir} not found")
        return
    
    print("Scanning generated docs directory...")
    files_by_dir = extract_markdown_files(docs_dir)
    
    # Display statistics
    total_files = sum(len(files) for files in files_by_dir.values())
    print(f"Directories found: {len(files_by_dir)}")
    print(f"Total files: {total_files}")
    
    # Display file count by directory
    for dir_name in sorted(files_by_dir.keys()):
        print(f"  {dir_name}: {len(files_by_dir[dir_name])} files")
    
    print("\nGenerating index.md...")
    content = generate_index_content(files_by_dir)
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ New index.md has been generated: {output_file}")

if __name__ == "__main__":
    main()