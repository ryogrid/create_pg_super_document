#!/usr/bin/env python3
"""
HTML to MD File Extension Replacer

Script to replace .html file extensions with .md in .md files within the current directory
"""

import os
import re
import glob
import shutil
from pathlib import Path
from typing import List, Tuple, Dict

def find_md_files(directory: str = ".") -> List[str]:
    """Search for .md files in the specified directory"""
    return glob.glob(os.path.join(directory, "*.md"))

def create_backup(file_path: str) -> str:
    """Create a backup of the file"""
    backup_path = f"{file_path}.bak"
    shutil.copy2(file_path, backup_path)
    return backup_path

def replace_html_extensions(content: str) -> Tuple[str, int]:
    """
    Replace .html file extensions with .md in text
    
    Returns:
        tuple: (replaced text, number of replacements)
    """
    patterns = [
        # Markdown link format: [text](filename.html)
        (r'\[([^\]]*)\]\(([^)]*?)\.html\)', r'[\1](\2.md)'),
        
        # HTML link format: href="filename.html"
        (r'href=["\']([^"\']*?)\.html["\']', r'href="\1.md"'),
        
        # General filename pattern: filename.html (separated by word boundaries)
        (r'\b([a-zA-Z0-9_-]+)\.html\b', r'\1.md'),
        
        # Relative path format: ./path/filename.html or ../path/filename.html
        (r'(\./|\.\./|/)([^/\s]+?)\.html\b', r'\1\2.md'),
    ]
    
    modified_content = content
    total_replacements = 0
    
    for pattern, replacement in patterns:
        modified_content, count = re.subn(pattern, replacement, modified_content)
        total_replacements += count
    
    return modified_content, total_replacements

def process_markdown_file(file_path: str, create_backup_flag: bool = True) -> Dict:
    """
    Process a single Markdown file
    
    Returns:
        dict: Processing result information
    """
    result = {
        'file': file_path,
        'original_size': 0,
        'modified_size': 0,
        'replacements': 0,
        'backup_created': False,
        'success': False,
        'error': None
    }
    
    try:
        # Read file
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        result['original_size'] = len(original_content)
        
        # Replace HTML extensions
        modified_content, replacements = replace_html_extensions(original_content)
        result['replacements'] = replacements
        result['modified_size'] = len(modified_content)
        
        # Update file only if there are changes
        if replacements > 0:
            # Create backup
            if create_backup_flag:
                backup_path = create_backup(file_path)
                result['backup_created'] = True
                result['backup_path'] = backup_path
            
            # Update file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
            
            result['success'] = True
        else:
            result['success'] = True  # No changes is also considered success
            
    except Exception as e:
        result['error'] = str(e)
    
    return result

def main():
    """Main processing"""
    print("HTML to MD File Extension Replacer")
    print("=" * 50)
    
    # Search for .md files in current directory
    md_files = find_md_files()
    
    if not md_files:
        print("No .md files found in current directory.")
        return
    
    print(f"Target files: {len(md_files)} files")
    print()
    
    # Confirmation
    response = input("Continue processing? [y/N]: ").strip().lower()
    if response not in ['y', 'yes']:
        print("Processing cancelled.")
        return
    
    # Backup confirmation
    backup_response = input("Create backups? [Y/n]: ").strip().lower()
    create_backup_flag = backup_response not in ['n', 'no']
    
    print()
    print("Processing...")
    print("-" * 50)
    
    # Process each file
    total_replacements = 0
    successful_files = 0
    failed_files = 0
    
    for file_path in md_files:
        result = process_markdown_file(file_path, create_backup_flag)
        
        if result['success']:
            successful_files += 1
            total_replacements += result['replacements']
            
            if result['replacements'] > 0:
                print(f"✓ {os.path.basename(file_path)}: {result['replacements']} replacements")
                if result['backup_created']:
                    print(f"  Backup: {os.path.basename(result['backup_path'])}")
            else:
                print(f"- {os.path.basename(file_path)}: No changes")
        else:
            failed_files += 1
            print(f"✗ {os.path.basename(file_path)}: Error - {result['error']}")
    
    # Result summary
    print("-" * 50)
    print("Processing completed")
    print(f"Success: {successful_files} files")
    print(f"Failed: {failed_files} files")
    print(f"Total replacements: {total_replacements} locations")
    
    if total_replacements > 0:
        print()
        print("Replacement patterns:")
        print("- [text](filename.html) → [text](filename.md)")
        print("- href=\"filename.html\" → href=\"filename.md\"")
        print("- filename.html → filename.md")
        print("- ./path/filename.html → ./path/filename.md")

def test_replacements():
    """Test the replacement functionality"""
    test_content = """
# Test Document

[Link1](page1.html) is an important page.
Please also refer to [Link2](./docs/page2.html).
There is an <a href="page3.html">HTML link</a>.

Filename: document.html
Relative path: ../other/file.html
Absolute path: /root/index.html

The page4.html file in normal text will also be converted.
"""
    
    print("=== Replacement Test ===")
    print("Original text:")
    print(test_content)
    print()
    
    modified_content, count = replace_html_extensions(test_content)
    
    print("After replacement:")
    print(modified_content)
    print(f"Number of replacements: {count}")

if __name__ == "__main__":
    # Test mode with command line argument
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_replacements()
    else:
        main()
