#!/usr/bin/env python3
"""
シンボル定義データベースにシンボルの種類を追加するスクリプト

このスクリプトは、snode_moduleを使用して各シンボルのソースコードを取得し、
それが関数、マクロ、変数、構造体、共用体、enum、typedefなどのいずれであるかを
判別して、データベースの 'symbol_type' カラムに記録します。
"""

import re
import duckdb
from tqdm import tqdm
import snode_module

# --- グローバル設定 ---
DB_FILE = "global_symbols.db"
SYMBOL_TABLE = "symbol_definitions"


def classify_source_code(source_code: str) -> str:
    """
    ソースコードの文字列からシンボルの種類を判定する

    Args:
        source_code: get_source_code()から取得したヘッダー付きのソースコード

    Returns:
        str: 'f', 'm', 'c', 's', 'k', 'e', 't', 'v', 'u' のいずれかの分類コード
    """
    # ヘッダー行（"Source: ..."）を除去
    lines = source_code.splitlines()
    code_lines = lines[1:] if lines and lines[0].startswith("Source:") else lines

    # コメントや空行を除いた、意味のある最初の行を探す
    first_meaningful_line = ""
    for line in code_lines:
        stripped_line = line.strip()
        if stripped_line and not stripped_line.startswith(('//', '/*')) and not stripped_line.startswith('*'):
            first_meaningful_line = stripped_line
            break
    
    # 解析しやすいように、複数行を一つの文字列に結合
    full_code = "\n".join(code_lines).strip()

    # 1. マクロの判定
    if first_meaningful_line.startswith("#define"):
        # #define SYMBOL( ... ) の形式かチェック
        if re.search(r'#define\s+\w+\(', first_meaningful_line):
            return 'm'  # 関数形式マクロ
        else:
            return 'c'  # 定数マクロ

    # 2. 構造体 (struct) の判定
    if re.match(r'^\s*(typedef\s+)?struct', first_meaningful_line, re.IGNORECASE):
        return 's'

    # 3. 共用体 (union) の判定
    if re.match(r'^\s*(typedef\s+)?union', first_meaningful_line, re.IGNORECASE):
        return 'k'

    # 4. 列挙型 (enum) の判定
    if re.match(r'^\s*(typedef\s+)?enum', first_meaningful_line, re.IGNORECASE):
        return 'e'

    # 5. その他の型定義 (typedef) の判定
    # struct, union, enumを伴わない純粋なtypedef
    if re.match(r'^\s*typedef', first_meaningful_line, re.IGNORECASE):
        return 't'

    # 6. 関数の判定
    # heuristic: 戻り値の型、関数名、引数リスト '()' の後に '{' が続くパターン
    if '{' in full_code and '}' in full_code:
        # struct/union/enum の定義は上記で判定済みなので除外は不要
        # 例: `int my_func(void) { ... }`
        if re.search(r'[\w\s\*]+\s+\w+\s*\([^;{}]*\)\s*\{', full_code, re.DOTALL):
            return 'f'  # 関数

    # 7. グローバル変数の判定
    # heuristic: 上記のいずれでもなく、文末が ';' で終わる
    if full_code.endswith(';'):
         return 'v' # グローバル変数

    # 8. 上記のいずれにも分類できないもの
    return 'u'


def main():
    """メイン処理"""
    print("Starting symbol type classification script.")

    # 1. データベースに接続
    try:
        # snode_moduleはread_onlyで接続するため、更新用に別途接続する
        db = snode_module.DatabaseConnection()
        con = db.get_connection()  # snode_moduleのシングルトン接続を初期化し取得
    except duckdb.IOException as e:
        print(f"Error: Could not connect to database '{DB_FILE}'. {e}")
        return

    # 2. 'symbol_type' カラムの存在を確認し、なければ追加・インデックス作成
    try:
        table_info = con.execute(f"PRAGMA table_info('{SYMBOL_TABLE}')").fetchall()
        column_names = [col[1] for col in table_info]

        if 'symbol_type' not in column_names:
            print(f"Adding 'symbol_type' column to '{SYMBOL_TABLE}' table...")
            con.execute(f"ALTER TABLE {SYMBOL_TABLE} ADD COLUMN symbol_type VARCHAR;")
            print("Creating index on 'symbol_type' column...")
            con.execute(f"CREATE INDEX idx_symbol_type ON {SYMBOL_TABLE} (symbol_type);")
            print("Column and index created successfully.")
        else:
            print("'symbol_type' column already exists.")
    except duckdb.Error as e:
        print(f"Error during database setup: {e}")
        con.close()
        return

    # 3. 全シンボルのIDを取得
    try:
        print("Fetching all symbol IDs...")
        all_ids = con.execute(f"SELECT id FROM {SYMBOL_TABLE} ORDER BY id").fetchall()
        symbol_ids = [row[0] for row in all_ids]
        print(f"Found {len(symbol_ids)} symbols to process.")
    except duckdb.Error as e:
        print(f"Error fetching symbol IDs: {e}")
        con.close()
        return

    # 4. 各シンボルを分類
    updates_to_perform = []
    print("\nClassifying symbol types...")
    for symbol_id in tqdm(symbol_ids, desc="Classifying"):
        try:
            node = snode_module.SNode.from_id(symbol_id)
            source_code = node.get_source_code()
            symbol_type = classify_source_code(source_code)
            updates_to_perform.append((symbol_type, symbol_id))
        except (ValueError, FileNotFoundError) as e:
            print(f"\nWarning: Skipping symbol ID {symbol_id} ({getattr(node, 'symbol_name', 'N/A')}). Reason: {e}")
        except Exception as e:
            print(f"\nError: An unexpected error occurred for symbol ID {symbol_id}: {e}")

    # 5. データベースを一括更新
    if updates_to_perform:
        print(f"\nUpdating {len(updates_to_perform)} records in the database...")
        try:
            con.begin() # トランザクション開始
            for symbol_type, symbol_id in tqdm(updates_to_perform, desc="Updating DB"):
                con.execute(f"UPDATE {SYMBOL_TABLE} SET symbol_type = ? WHERE id = ?", (symbol_type, symbol_id))
            con.commit() # トランザクション確定
            print("Database update completed successfully.")
        except duckdb.Error as e:
            print(f"Error during database update: {e}")
            con.rollback()
            print("Changes were rolled back.")
    else:
        print("No symbols were classified or updated.")

    ## 接続を閉じる
    #con.close()
    # snode_moduleが使用するシングルトン接続も閉じる
    snode_module.DatabaseConnection().close()
    print("Script finished.")


if __name__ == "__main__":
    main()
