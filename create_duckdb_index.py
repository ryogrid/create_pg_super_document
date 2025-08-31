#!/usr/bin/env python3
"""
GNU GLOBALのインデックス情報をDuckDBデータベースに整理するスクリプト
"""

import os
import sys
import subprocess
import duckdb
from pathlib import Path
from typing import List, Tuple, Optional

# データベース設定
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_definitions"


def create_table_if_not_exists(conn: duckdb.DuckDBPyConnection) -> None:
    """テーブルが存在しない場合、テーブルとインデックスを作成"""
    
    # テーブル作成
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY,
            symbol_name VARCHAR NOT NULL,
            file_path VARCHAR NOT NULL,
            line_num_start INTEGER NOT NULL,
            line_num_end INTEGER NOT NULL DEFAULT 0,
            line_content VARCHAR NOT NULL,
            contents VARCHAR DEFAULT ''
        )
    """)
    
    # インデックス作成（既に存在する場合はエラーを無視）
    try:
        conn.execute(f"CREATE INDEX idx_symbol_name ON {TABLE_NAME} (symbol_name)")
    except:
        pass  # インデックスが既に存在する場合
    
    try:
        conn.execute(f"CREATE INDEX idx_file_line_start ON {TABLE_NAME} (file_path, line_num_start)")
    except:
        pass
    
    try:
        conn.execute(f"CREATE INDEX idx_file_line_end ON {TABLE_NAME} (file_path, line_num_end)")
    except:
        pass


def get_next_id(conn: duckdb.DuckDBPyConnection) -> int:
    """次のIDを取得（単調増加）"""
    result = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {TABLE_NAME}").fetchone()
    return result[0]


def get_processed_files(conn: duckdb.DuckDBPyConnection) -> set:
    """既に処理済みのファイルパスのセットを取得"""
    result = conn.execute(f"SELECT DISTINCT file_path FROM {TABLE_NAME}").fetchall()
    return {row[0] for row in result}


def find_c_and_h_files(src_dir: Path) -> List[Path]:
    """srcディレクトリ内のCファイルとヘッダファイルを再帰的に検索"""
    files = []
    for ext in ['*.c', '*.h']:
        files.extend(src_dir.rglob(ext))
    return sorted(files)


def parse_global_output(output: str, file_path: str) -> List[Tuple[str, str, int, str]]:
    """
    globalコマンドの出力を解析
    Returns: List of (symbol_name, file_path, line_num, line_content)
    """
    results = []
    for line in output.strip().split('\n'):
        if not line:
            continue
        
        # スペースで分割（最低4要素必要）
        parts = line.split(None, 3)
        if len(parts) < 4:
            print(f"Warning: Skipping malformed line: {line}", file=sys.stderr)
            continue
        
        symbol_name = parts[0]
        try:
            line_num = int(parts[1])
        except ValueError:
            print(f"Warning: Invalid line number in: {line}", file=sys.stderr)
            continue
        
        # parts[2]はファイルパス（コマンドで指定したものと同じはず）
        # parts[3]以降が行の内容
        line_content = parts[3]
        
        results.append((symbol_name, file_path, line_num, line_content))
    
    return results


def run_global_command(file_path: Path) -> Optional[str]:
    """globalコマンドを実行してシンボル情報を取得"""
    try:
        # GNU GLOBALのインデックスがあるディレクトリで実行
        result = subprocess.run(
            ['global', '-fx', str(file_path)],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running global command for {file_path}: {e}", file=sys.stderr)
        return None
    except FileNotFoundError:
        print("Error: 'global' command not found. Please ensure GNU GLOBAL is installed.", file=sys.stderr)
        sys.exit(1)


def insert_symbols(conn: duckdb.DuckDBPyConnection, symbols: List[Tuple[str, str, int, str]], 
                   start_id: int) -> int:
    """シンボル情報をデータベースに挿入"""
    current_id = start_id
    
    for symbol_name, file_path, line_num, line_content in symbols:
        conn.execute(f"""
            INSERT INTO {TABLE_NAME} 
            (id, symbol_name, file_path, line_num_start, line_num_end, line_content, contents)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (current_id, symbol_name, file_path, line_num, 0, line_content, ''))
        current_id += 1
    
    return current_id


def main():
    """メイン処理"""
    # srcディレクトリの確認
    src_dir = Path.cwd() / 'src'
    if not src_dir.exists():
        print(f"Error: 'src' directory not found in current directory: {Path.cwd()}", file=sys.stderr)
        sys.exit(1)
    
    # データベース接続
    conn = duckdb.connect(DB_FILE)
    
    try:
        # テーブル作成（必要な場合）
        create_table_if_not_exists(conn)
        
        # 処理済みファイルのセットを取得
        processed_files = get_processed_files(conn)
        if processed_files:
            print(f"Found {len(processed_files)} already processed files. Continuing from where we left off...")
        
        # 次のIDを取得
        next_id = get_next_id(conn)
        
        # CファイルとHファイルを検索
        files = find_c_and_h_files(src_dir)
        print(f"Found {len(files)} C/H files in {src_dir}")
        
        # 各ファイルを処理
        processed_count = 0
        skipped_count = 0
        total_symbols = 0
        
        for file_path in files:
            file_path_str = str(file_path)
            
            # 既に処理済みの場合はスキップ
            if file_path_str in processed_files:
                skipped_count += 1
                continue
            
            print(f"Processing: {file_path_str}")
            
            # globalコマンドを実行
            output = run_global_command(file_path)
            if output is None:
                continue
            
            # 出力を解析
            symbols = parse_global_output(output, file_path_str)
            
            if symbols:
                # データベースに挿入
                next_id = insert_symbols(conn, symbols, next_id)
                total_symbols += len(symbols)
                print(f"  -> Inserted {len(symbols)} symbols")
            
            processed_count += 1
            
            # 定期的にコミット
            if processed_count % 10 == 0:
                conn.commit()
        
        # 最終コミット
        conn.commit()
        
        print("\n" + "="*50)
        print(f"Processing complete!")
        print(f"  Files processed: {processed_count}")
        print(f"  Files skipped (already processed): {skipped_count}")
        print(f"  Total symbols inserted: {total_symbols}")
        print(f"  Database: {DB_FILE}")
        print(f"  Table: {TABLE_NAME}")
        
        # 統計情報を表示
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        unique_symbols = conn.execute(f"SELECT COUNT(DISTINCT symbol_name) FROM {TABLE_NAME}").fetchone()[0]
        unique_files = conn.execute(f"SELECT COUNT(DISTINCT file_path) FROM {TABLE_NAME}").fetchone()[0]
        
        print(f"\nDatabase statistics:")
        print(f"  Total records: {total_rows}")
        print(f"  Unique symbols: {unique_symbols}")
        print(f"  Unique files: {unique_files}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
