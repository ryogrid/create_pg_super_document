#!/usr/bin/env python3
"""
symbol_definitionsテーブルのline_num_endを設定し、重複を除去するスクリプト
"""

import sys
import duckdb
from pathlib import Path
from typing import List, Tuple, Dict

# データベース設定
DB_FILE = "global_symbols.db"
TABLE_NAME = "symbol_definitions"


def process_line_num_end(conn: duckdb.DuckDBPyConnection) -> None:
    """
    処理1: line_num_endを設定する
    """
    print("=" * 60)
    print("Processing line_num_end values...")
    print("=" * 60)
    
    # IDの昇順で全レコードを取得
    records = conn.execute(f"""
        SELECT id, symbol_name, file_path, line_num_start, line_num_end
        FROM {TABLE_NAME}
        ORDER BY id
    """).fetchall()
    
    if not records:
        print("No records found in the table.")
        return
    
    # 削除するIDのリスト
    ids_to_delete = []
    # 更新するレコードのリスト: (id, line_num_end)
    updates = []
    
    i = 0
    while i < len(records) - 1:
        current = records[i]
        next_rec = records[i + 1]
        
        current_id = current[0]
        current_symbol = current[1]
        current_file = current[2]
        current_line_start = current[3]
        
        next_id = next_rec[0]
        next_symbol = next_rec[1]
        next_file = next_rec[2]
        next_line_start = next_rec[3]
        
        # file_pathが同一の連続するレコード
        if current_file == next_file:
            # symbol_nameも同一の場合（構造体のtypedef対応）
            if current_symbol == next_symbol:
                # 現在のレコードのline_num_endに次のレコードのline_num_startを設定
                updates.append((current_id, next_line_start))
                # 次のレコードを削除対象に追加
                ids_to_delete.append(next_id)
                print(f"  Merging typedef: {current_symbol} in {current_file} (lines {current_line_start}-{next_line_start})")
            else:
                # 通常の連続レコード: line_num_end = next_line_start - 1
                updates.append((current_id, next_line_start - 1))
        
        i += 1
    
    # 最後のレコードまたはファイルの最後のレコードはline_num_endを0のまま残す（または適切な値を設定）
    # ここでは処理しない（0のまま）
    
    # 更新を実行
    print(f"\nApplying {len(updates)} updates...")
    for id_val, line_end in updates:
        conn.execute(f"""
            UPDATE {TABLE_NAME}
            SET line_num_end = ?
            WHERE id = ?
        """, (line_end, id_val))
    
    # 削除を実行
    if ids_to_delete:
        print(f"Deleting {len(ids_to_delete)} merged records...")
        for id_val in ids_to_delete:
            conn.execute(f"DELETE FROM {TABLE_NAME} WHERE id = ?", (id_val,))
    
    conn.commit()
    print(f"Processing complete: {len(updates)} records updated, {len(ids_to_delete)} records deleted")


def process_symbol_duplicates(conn: duckdb.DuckDBPyConnection) -> None:
    """
    処理2: symbol_nameの重複を取り除く
    """
    print("\n" + "=" * 60)
    print("Processing symbol name duplicates...")
    print("=" * 60)
    
    # symbol_nameでソートして全レコードを取得
    records = conn.execute(f"""
        SELECT id, symbol_name, file_path, line_num_start, line_num_end, line_content
        FROM {TABLE_NAME}
        ORDER BY symbol_name, file_path, line_num_start
    """).fetchall()
    
    if not records:
        print("No records found in the table.")
        return
    
    # 削除するIDのリスト
    ids_to_delete = []
    # 処理できない重複のリスト
    unhandled_duplicates = []
    
    i = 0
    while i < len(records) - 1:
        current = records[i]
        next_rec = records[i + 1]
        
        current_id = current[0]
        current_symbol = current[1]
        current_file = current[2]
        current_line_start = current[3]
        current_line_content = current[5]
        
        next_id = next_rec[0]
        next_symbol = next_rec[1]
        next_file = next_rec[2]
        next_line_start = next_rec[3]
        next_line_content = next_rec[5]
        
        # symbol_nameが同一の連続レコード
        if current_symbol == next_symbol:
            # ファイル拡張子を取得
            current_ext = Path(current_file).suffix
            next_ext = Path(next_file).suffix
            
            # hファイルとcファイルの組み合わせ
            if {current_ext, next_ext} == {'.h', '.c'}:
                # hファイルのレコードを削除
                if current_ext == '.h':
                    ids_to_delete.append(current_id)
                    print(f"  Removing extern declaration: {current_symbol} from {current_file}")
                else:
                    ids_to_delete.append(next_id)
                    print(f"  Removing extern declaration: {next_symbol} from {next_file}")
            
            # 両方ともcファイルで、かつ同一ファイル
            elif current_ext == '.c' and next_ext == '.c' and current_file == next_file:
                # line_num_startが小さい方を削除（プロトタイプ宣言と見なす）
                if current_line_start < next_line_start:
                    ids_to_delete.append(current_id)
                    print(f"  Removing prototype: {current_symbol} at line {current_line_start} in {current_file}")
                else:
                    ids_to_delete.append(next_id)
                    print(f"  Removing prototype: {next_symbol} at line {next_line_start} in {next_file}")
            
            # 上記のいずれにも当てはまらない場合
            else:
                unhandled_duplicates.append((current, next_rec))
        
        i += 1
    
    # 処理できない重複を標準エラー出力に出力
    if unhandled_duplicates:
        print("\n" + "=" * 60, file=sys.stderr)
        print("WARNING: Unhandled duplicate symbols:", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        for current, next_rec in unhandled_duplicates:
            print(f"\nDuplicate symbol: {current[1]}", file=sys.stderr)
            print(f"  Record 1: ID={current[0]}, File={current[2]}, Line={current[3]}", file=sys.stderr)
            print(f"    Content: {current[5][:80]}...", file=sys.stderr)
            print(f"  Record 2: ID={next_rec[0]}, File={next_rec[2]}, Line={next_rec[3]}", file=sys.stderr)
            print(f"    Content: {next_rec[5][:80]}...", file=sys.stderr)
    
    # 削除を実行
    if ids_to_delete:
        print(f"\nDeleting {len(ids_to_delete)} duplicate records...")
        for id_val in ids_to_delete:
            conn.execute(f"DELETE FROM {TABLE_NAME} WHERE id = ?", (id_val,))
    
    conn.commit()
    print(f"Processing complete: {len(ids_to_delete)} records deleted, {len(unhandled_duplicates)} unhandled duplicates")


def show_statistics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    処理後の統計情報を表示
    """
    print("\n" + "=" * 60)
    print("Database Statistics")
    print("=" * 60)
    
    # 総レコード数
    total_records = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Total records: {total_records}")
    
    # ユニークシンボル数
    unique_symbols = conn.execute(f"SELECT COUNT(DISTINCT symbol_name) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Unique symbols: {unique_symbols}")
    
    # ユニークファイル数
    unique_files = conn.execute(f"SELECT COUNT(DISTINCT file_path) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Unique files: {unique_files}")
    
    # line_num_endが設定されたレコード数
    records_with_end = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE line_num_end > 0").fetchone()[0]
    print(f"Records with line_num_end set: {records_with_end}")
    
    # ファイル拡張子別の統計
    print("\nRecords by file extension:")
    ext_stats = conn.execute(f"""
        SELECT 
            CASE 
                WHEN file_path LIKE '%.h' THEN '.h'
                WHEN file_path LIKE '%.c' THEN '.c'
                ELSE 'other'
            END as ext,
            COUNT(*) as count
        FROM {TABLE_NAME}
        GROUP BY ext
        ORDER BY count DESC
    """).fetchall()
    
    for ext, count in ext_stats:
        print(f"  {ext}: {count}")
    
    # 重複シンボルの統計
    print("\nSymbols with multiple definitions:")
    duplicates = conn.execute(f"""
        SELECT symbol_name, COUNT(*) as count
        FROM {TABLE_NAME}
        GROUP BY symbol_name
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    
    if duplicates:
        for symbol, count in duplicates:
            print(f"  {symbol}: {count} definitions")
    else:
        print("  No duplicate symbols found")


def main():
    """メイン処理"""
    # データベースファイルの確認
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.", file=sys.stderr)
        print("Please run global_to_duckdb.py first to create the database.", file=sys.stderr)
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
        print("Initial database state:")
        show_statistics(conn)
        
        # 処理1: line_num_endを設定
        process_line_num_end(conn)
        
        # 処理2: symbol_nameの重複を除去
        process_symbol_duplicates(conn)
        
        # 処理後の統計
        print("\nFinal database state:")
        show_statistics(conn)
        
        print("\n" + "=" * 60)
        print("All processing completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
