#!/usr/bin/env python3
"""
GNU GLOBALの参照情報を使用してシンボル間の参照関係をCSVファイルに整理するスクリプト
"""

import sys
import csv
import subprocess
import duckdb
from pathlib import Path
from typing import Optional, List, Tuple, Set

# データベース設定
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_definitions"
OUTPUT_CSV = "symbol_references.csv"


def run_global_rx_command(symbol_name: str) -> Optional[str]:
    """global -rx コマンドを実行してシンボルの参照情報を取得"""
    try:
        result = subprocess.run(
            ['global', '-rx', symbol_name],
            capture_output=True,
            text=True,
            check=False  # シンボルが見つからない場合もエラーにしない
        )
        # 出力がある場合のみ返す
        if result.stdout.strip():
            return result.stdout
        return None
    except FileNotFoundError:
        print("Error: 'global' command not found. Please ensure GNU GLOBAL is installed.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error running global -rx for '{symbol_name}': {e}", file=sys.stderr)
        return None


def parse_global_rx_output(output: str) -> List[Tuple[str, str, int]]:
    """
    global -rx の出力を解析
    Returns: List of (symbol_name, file_path, line_num)
    """
    results = []
    for line in output.strip().split('\n'):
        if not line:
            continue
        
        # スペースで分割（最低3要素必要）
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        
        symbol = parts[0]
        try:
            line_num = int(parts[1])
        except ValueError:
            continue
        
        file_path = parts[2]
        results.append((symbol, file_path, line_num))
    
    return results


def find_referencing_symbol_id(conn: duckdb.DuckDBPyConnection, 
                               file_path: str, 
                               line_num: int) -> Optional[int]:
    """
    指定されたファイルパスと行番号から、その位置を含むシンボル定義のIDを検索
    """
    # line_num_end = 0 の場合も考慮（ファイル末尾まで）
    query = f"""
        SELECT id 
        FROM {TABLE_NAME}
        WHERE file_path = ?
          AND line_num_start <= ?
          AND (line_num_end >= ? OR line_num_end = 0)
        ORDER BY id
        LIMIT 1
    """
    
    result = conn.execute(query, (file_path, line_num, line_num)).fetchone()
    if result:
        return result[0]
    return None


def get_symbol_definition_id(conn: duckdb.DuckDBPyConnection, 
                             symbol_name: str) -> Optional[int]:
    """
    指定されたシンボル名に対応する最小のIDを取得
    """
    query = f"""
        SELECT MIN(id) 
        FROM {TABLE_NAME}
        WHERE symbol_name = ?
    """
    
    result = conn.execute(query, (symbol_name,)).fetchone()
    if result and result[0] is not None:
        return result[0]
    return None


def process_symbol_references(conn: duckdb.DuckDBPyConnection) -> List[Tuple[int, int, int]]:
    """
    全シンボルの参照関係を処理
    Returns: List of (referencing_id, referenced_id, line_num)
    """
    references = []
    
    # ユニークなシンボル名を取得（symbol_nameでソート）
    unique_symbols = conn.execute(f"""
        SELECT DISTINCT symbol_name 
        FROM {TABLE_NAME}
        ORDER BY symbol_name
    """).fetchall()
    
    total_symbols = len(unique_symbols)
    print(f"Processing {total_symbols} unique symbols...")
    
    processed_count = 0
    found_references = 0
    
    for (symbol_name,) in unique_symbols:
        processed_count += 1
        
        # 進捗表示
        if processed_count % 100 == 0:
            print(f"  Progress: {processed_count}/{total_symbols} symbols processed, "
                  f"{found_references} references found...")
        
        # global -rx コマンドを実行
        output = run_global_rx_command(symbol_name)
        if not output:
            continue
        
        # 出力を解析
        reference_locations = parse_global_rx_output(output)
        if not reference_locations:
            continue
        
        # 参照先（定義）のIDを取得
        referenced_id = get_symbol_definition_id(conn, symbol_name)
        if referenced_id is None:
            print(f"  Warning: No definition found for symbol '{symbol_name}'")
            continue
        
        # 各参照位置について処理
        for _, file_path, line_num in reference_locations:
            # 参照元のIDを検索
            referencing_id = find_referencing_symbol_id(conn, file_path, line_num)
            
            if referencing_id is not None:
                # 自己参照（定義内での参照）は除外するオプション
                # if referencing_id != referenced_id:
                references.append((referencing_id, referenced_id, line_num))
                found_references += 1
    
    print(f"\nProcessing complete: {processed_count} symbols processed, "
          f"{found_references} references found")
    
    return references


def write_csv(references: List[Tuple[int, int, int]], output_file: str) -> None:
    """参照関係をCSVファイルに書き出し"""
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # ヘッダーは書かない（要件通り）
        # writer.writerow(['referencing_id', 'referenced_id', 'line_num'])
        
        for ref in references:
            writer.writerow(ref)
    
    print(f"Written {len(references)} references to {output_file}")


def show_statistics(conn: duckdb.DuckDBPyConnection, 
                   references: List[Tuple[int, int, int]]) -> None:
    """統計情報を表示"""
    print("\n" + "=" * 60)
    print("Statistics")
    print("=" * 60)
    
    if not references:
        print("No references found")
        return
    
    # 基本統計
    print(f"Total references: {len(references)}")
    
    # 参照元と参照先のユニーク数
    referencing_ids = set(ref[0] for ref in references)
    referenced_ids = set(ref[1] for ref in references)
    
    print(f"Unique referencing symbols: {len(referencing_ids)}")
    print(f"Unique referenced symbols: {len(referenced_ids)}")
    
    # 最も多く参照されているシンボルTOP10
    reference_counts = {}
    for _, referenced_id, _ in references:
        reference_counts[referenced_id] = reference_counts.get(referenced_id, 0) + 1
    
    top_referenced = sorted(reference_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    if top_referenced:
        print("\nTop 10 most referenced symbols:")
        for ref_id, count in top_referenced:
            # シンボル名を取得
            result = conn.execute(f"""
                SELECT symbol_name, file_path, line_num_start
                FROM {TABLE_NAME}
                WHERE id = ?
            """, (ref_id,)).fetchone()
            
            if result:
                symbol_name, file_path, line_start = result
                # ファイルパスを短縮表示
                if len(file_path) > 40:
                    short_path = "..." + file_path[-37:]
                else:
                    short_path = file_path
                print(f"  {symbol_name:30s} ({short_path}:{line_start}) - {count} references")


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
        
        # テーブルの基本情報を表示
        total_symbols = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        unique_symbols = conn.execute(f"SELECT COUNT(DISTINCT symbol_name) FROM {TABLE_NAME}").fetchone()[0]
        
        print("=" * 60)
        print("Symbol Reference Extraction")
        print("=" * 60)
        print(f"Database: {DB_FILE}")
        print(f"Table: {TABLE_NAME}")
        print(f"Total symbol definitions: {total_symbols}")
        print(f"Unique symbol names: {unique_symbols}")
        print(f"Output file: {OUTPUT_CSV}")
        print()
        
        # 参照関係を処理
        references = process_symbol_references(conn)
        
        # CSVファイルに書き出し
        if references:
            write_csv(references, OUTPUT_CSV)
            
            # 統計情報を表示
            show_statistics(conn, references)
        else:
            print("No references found to write to CSV")
        
        print("\n" + "=" * 60)
        print("Processing completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
