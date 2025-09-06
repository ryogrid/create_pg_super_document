#!/usr/bin/env python3
"""
C言語のコードツリーを解析し、ドキュメント情報をDuckDBに格納するスクリプト。

- src/ と contrib/ ディレクトリを再帰的に走査します。
- 各ディレクトリのREADME*ファイルの内容を'dir_info'テーブルに集約します。
- 各C/Hファイルのヘッダーコメントを'file_info'テーブルに抽出します。
"""

import duckdb
import os
import re
from pathlib import Path
from typing import List, Optional

# --- 設定 ---
DB_FILE = "assistive_info.db"
TARGET_DIRS = ["src", "contrib"]
README_SEPARATOR = "\n\n---\n\n"

def setup_database(conn: duckdb.DuckDBPyConnection):
    """データベースとテーブルの初期設定を行う"""
    print("Setting up database tables...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dir_info (
            path VARCHAR PRIMARY KEY,
            readme_contents VARCHAR
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_info (
            path VARCHAR PRIMARY KEY,
            header_comment VARCHAR
        );
    """)
    print("Database setup complete.")

def extract_header_comment(file_path: Path) -> Optional[str]:
    """
    ファイルの先頭からC形式のブロックコメントを抽出し、整形する。
    コメントが見つからないか、コードがコメントより先にある場合はNoneを返す。
    """
    try:
        with file_path.open('r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except IOError:
        return None

    comment_lines = []
    in_comment = False

    for line in lines:
        stripped_line = line.strip()

        # コメントが開始される前にコードやプリプロセッサ命令があれば終了
        if not in_comment and stripped_line and not stripped_line.startswith('/*'):
            return None

        if stripped_line.startswith('/*'):
            in_comment = True
        
        if in_comment:
            comment_lines.append(line)

        if '*/' in stripped_line:
            break
    
    if not comment_lines:
        return None

    # コメントブロック全体を一つの文字列に
    full_comment = "".join(comment_lines)
    
    # /* と */ 及びその間のハイフンなどを除去
    match = re.search(r'/\*(-*)\n(.*?)\n\s*(-*)\*/', full_comment, re.DOTALL)
    if not match:
        # シンプルな /* comment */ 形式の場合
        match = re.search(r'/\*\s*(.*?)\s*\*/', full_comment, re.DOTALL)
        if not match:
            return None # 期待する形式のコメントではない
        content = match.group(1)
    else:
        content = match.group(2)

    # 各行の先頭にある '*' を除去
    lines_without_stars = [re.sub(r'^\s*\*\s?', '', line) for line in content.splitlines()]
    
    # ★★★ 修正点 ★★★
    # "Copyright" という文字列を含む行を除去 (大文字小文字を区別しない)
    final_lines = [line for line in lines_without_stars if 'copyright' not in line.lower()]

    # 最終的な文字列を生成
    final_comment = "\n".join(final_lines).strip()
    
    # Copyright行などを除去した結果、コメントが空になった場合はNoneを返す
    return final_comment if final_comment else None

def process_directory(base_path: Path, conn: duckdb.DuckDBPyConnection):
    """指定されたディレクトリを再帰的に処理する"""
    if not base_path.is_dir():
        print(f"Warning: Directory '{base_path}' not found. Skipping.")
        return

    print(f"Processing directory: {base_path}...")
    
    cwd = Path.cwd()

    for dirpath, _, filenames in os.walk(base_path):
        current_dir = Path(dirpath)
        relative_dir_path = current_dir.relative_to(cwd).as_posix()

        # --- READMEファイルの処理 ---
        readme_files = sorted([
            f for f in filenames if f.lower().startswith('readme')
        ])

        if readme_files:
            all_readme_contents = []
            for fname in readme_files:
                try:
                    content = (current_dir / fname).read_text(encoding='utf-8', errors='ignore')
                    all_readme_contents.append(content)
                except Exception as e:
                    print(f"Warning: Could not read {current_dir / fname}: {e}")
            
            if all_readme_contents:
                aggregated_content = README_SEPARATOR.join(all_readme_contents)
                conn.execute(
                    "INSERT INTO dir_info (path, readme_contents) VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET readme_contents = excluded.readme_contents",
                    (relative_dir_path, aggregated_content)
                )

        # --- .c, .h ファイルの処理 ---
        for filename in filenames:
            if filename.endswith(('.c', '.h')):
                file_path = current_dir / filename
                relative_file_path = file_path.relative_to(cwd).as_posix()
                
                comment = extract_header_comment(file_path)
                if comment:
                    conn.execute(
                        "INSERT INTO file_info (path, header_comment) VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET header_comment = excluded.header_comment",
                        (relative_file_path, comment)
                    )

def main():
    """メイン処理"""
    print("Starting codebase analysis script.")
    try:
        with duckdb.connect(DB_FILE) as conn:
            setup_database(conn)
            
            for dir_name in TARGET_DIRS:
                absolute_path = Path.cwd() / dir_name
                process_directory(absolute_path, conn)

        print("\nAnalysis complete.")
        print(f"Results have been stored in '{DB_FILE}'.")

    except duckdb.Error as e:
        print(f"A database error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
