#!/usr/bin/env python3
"""
line_num_endが0のレコードのうち、ファイル内最後のシンボルについて
ファイルの総行数をline_num_endに設定するスクリプト
"""

import sys
import subprocess
import duckdb
from pathlib import Path
from typing import Optional, List, Tuple

# データベース設定
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_definitions"


def run_global_command(file_path: str) -> Optional[str]:
    """globalコマンドを実行してシンボル情報を取得"""
    try:
        result = subprocess.run(
            ['global', '-fx', file_path],
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


def get_last_symbol_from_global(output: str) -> Optional[str]:
    """globalコマンドの出力から最後のシンボル名を取得"""
    if not output:
        return None
    
    lines = output.strip().split('\n')
    if not lines:
        return None
    
    # 最後の行を解析
    last_line = lines[-1]
    parts = last_line.split(None, 3)  # スペースで分割（最大4要素）
    
    if len(parts) >= 1:
        return parts[0]  # シンボル名
    
    return None


def count_file_lines(file_path: str) -> int:
    """ファイルの総行数を取得"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Error counting lines in {file_path}: {e}", file=sys.stderr)
        return 0


def process_zero_end_lines(conn: duckdb.DuckDBPyConnection) -> None:
    """line_num_endが0のレコードを処理"""
    
    print("=" * 60)
    print("Processing records with line_num_end = 0")
    print("=" * 60)
    
    # line_num_endが0のレコードを取得
    records = conn.execute(f"""
        SELECT id, symbol_name, file_path, line_num_start
        FROM {TABLE_NAME}
        WHERE line_num_end = 0
        ORDER BY file_path, id
    """).fetchall()
    
    if not records:
        print("No records found with line_num_end = 0")
        return
    
    print(f"Found {len(records)} records with line_num_end = 0")
    
    # ファイルごとにグループ化して処理
    file_groups = {}
    for record in records:
        file_path = record[2]
        if file_path not in file_groups:
            file_groups[file_path] = []
        file_groups[file_path].append(record)
    
    print(f"Processing {len(file_groups)} unique files...")
    
    updates = []  # (id, line_num_end) のリスト
    processed_files = 0
    
    for file_path, file_records in file_groups.items():
        processed_files += 1
        
        # ファイルが存在するか確認
        if not Path(file_path).exists():
            print(f"  Warning: File not found: {file_path}")
            continue
        
        # globalコマンドを実行
        output = run_global_command(file_path)
        if output is None:
            continue
        
        # 最後のシンボルを取得
        last_symbol = get_last_symbol_from_global(output)
        if last_symbol is None:
            print(f"  Warning: Could not determine last symbol for {file_path}")
            continue
        
        # このファイルのレコードから最後のシンボルと一致するものを探す
        last_symbol_found = False
        for record in file_records:
            record_id = record[0]
            symbol_name = record[1]
            
            if symbol_name == last_symbol:
                # ファイルの総行数を取得
                total_lines = count_file_lines(file_path)
                if total_lines > 0:
                    updates.append((record_id, total_lines))
                    print(f"  {file_path}: Setting line_num_end={total_lines} for symbol '{symbol_name}' (last symbol in file)")
                    last_symbol_found = True
                    break
        
        if not last_symbol_found:
            # 最後のシンボルがline_num_end=0のレコードの中になかった場合
            # (既に他の処理でline_num_endが設定されている可能性がある)
            print(f"  {file_path}: Last symbol '{last_symbol}' already has line_num_end set or not found")
        
        # 進捗表示
        if processed_files % 100 == 0:
            print(f"  Progress: {processed_files}/{len(file_groups)} files processed...")
    
    # 更新を実行
    if updates:
        print(f"\nApplying {len(updates)} updates...")
        for id_val, line_end in updates:
            conn.execute(f"""
                UPDATE {TABLE_NAME}
                SET line_num_end = ?
                WHERE id = ?
            """, (line_end, id_val))
        
        conn.commit()
        print(f"Successfully updated {len(updates)} records")
    else:
        print("No updates needed")


def show_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    """統計情報を表示"""
    print("\n" + "=" * 60)
    print("Statistics")
    print("=" * 60)
    
    # line_num_endの統計
    total_records = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    records_with_end = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE line_num_end > 0").fetchone()[0]
    records_without_end = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE line_num_end = 0").fetchone()[0]
    
    print(f"Total records: {total_records}")
    print(f"Records with line_num_end > 0: {records_with_end}")
    print(f"Records with line_num_end = 0: {records_without_end}")
    
    if records_with_end > 0:
        percentage = (records_with_end / total_records) * 100
        print(f"Completion rate: {percentage:.2f}%")
    
    # line_num_end = 0のレコードの詳細（最初の10件）
    if records_without_end > 0:
        print(f"\nSample of remaining records with line_num_end = 0:")
        remaining = conn.execute(f"""
            SELECT symbol_name, file_path, line_num_start
            FROM {TABLE_NAME}
            WHERE line_num_end = 0
            ORDER BY file_path, line_num_start
            LIMIT 10
        """).fetchall()
        
        for symbol, file_path, line_start in remaining:
            # ファイルパスを短縮表示
            short_path = file_path
            if len(file_path) > 60:
                short_path = "..." + file_path[-57:]
            print(f"  {symbol:30s} {short_path:60s} line {line_start}")


def main():
    """メイン処理"""
    # データベースファイルの確認
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # データベース接続
    conn = duckdb.connect(DB_FILE)
    
    try:
        # テーブルの存在確認
        table_exists = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{TABLE_NAME}'
        """).fetchone()[0]
        
        if not table_exists:
            print(f"Error: Table '{TABLE_NAME}' not found in database.", file=sys.stderr)
            sys.exit(1)
        
        # 処理前の統計
        print("Initial state:")
        show_statistics(conn)
        
        # line_num_end = 0のレコードを処理
        process_zero_end_lines(conn)
        
        # 処理後の統計
        print("\nFinal state:")
        show_statistics(conn)
        
        print("\n" + "=" * 60)
        print("Processing completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
