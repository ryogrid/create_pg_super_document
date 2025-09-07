#!/usr/bin/env python3
"""
SNodeモジュール - シンボル情報を扱うクラスを提供
"""

import os
import sys
import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from functools import lru_cache

# データベース設定
DB_FILE = "global_symbols.db"
SYMBOL_TABLE = "symbol_definitions"
REFERENCE_TABLE = "symbol_reference"


class DatabaseConnection:
    """データベース接続を管理するシングルトンクラス"""
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_connection(self):
        """データベース接続を取得"""
        if self._connection is None:
            if not Path(DB_FILE).exists():
                raise FileNotFoundError(f"Database file '{DB_FILE}' not found.")
            self._connection = duckdb.connect(DB_FILE, read_only=False)
        return self._connection
    
    def close(self):
        """データベース接続を閉じる"""
        if self._connection:
            self._connection.close()
            self._connection = None


class SNode:
    """シンボル情報を表すノードクラス"""
    
    # データベース接続（クラス変数）
    _db = DatabaseConnection()
    
    def __init__(self, symbol_name: str):
        """
        シンボル名からSNodeオブジェクトを作成
        
        Args:
            symbol_name: シンボル名
        """
        self.symbol_name = symbol_name
        self._contents = None  # 遅延読み込み用
        
        # データベースからシンボル情報を取得（最小IDのレコードを使用）
        conn = self._db.get_connection()
        result = conn.execute(f"""
            SELECT id, file_path, line_num_start, line_num_end, symbol_type
            FROM {SYMBOL_TABLE}
            WHERE symbol_name = ?
            ORDER BY id
            LIMIT 1
        """, (symbol_name,)).fetchone()
        
        if not result:
            raise ValueError(f"Symbol '{symbol_name}' not found in database")
        
        # フィールドに格納
        self.id = result[0]
        self.file_path = result[1]
        self.line_num_start = result[2]
        self.line_num_end = result[3]
        self.symbol_type = result[4]

    @classmethod
    def from_id(cls, record_id: int) -> 'SNode':
        """
        レコードIDからSNodeオブジェクトを作成するファクトリ関数
        
        Args:
            record_id: symbol_definitionsテーブルのID
            
        Returns:
            SNode: 作成されたSNodeオブジェクト
        """
        conn = cls._db.get_connection()
        result = conn.execute(f"""
            SELECT symbol_name, file_path, line_num_start, line_num_end
            FROM {SYMBOL_TABLE}
            WHERE id = ?
        """, (record_id,)).fetchone()
        
        if not result:
            raise ValueError(f"Record with ID {record_id} not found in database")
        
        # SNodeオブジェクトを作成（コンストラクタを迂回して直接設定）
        node = object.__new__(cls)
        node.id = record_id
        node.symbol_name = result[0]
        node.file_path = result[1]
        node.line_num_start = result[2]
        node.line_num_end = result[3]
        node._contents = None
        
        return node
    
    def get_source_code(self) -> str:
        """
        自身のシンボルのソースコードを文字列として返す
        コメントや返り値の型を含み、次のシンボルのコメントは除外する
        
        Returns:
            str: ソースコード（ヘッダー情報付き）
        """
        # 既に読み込み済みの場合はそれを返す
        if self._contents is not None:
            return self._contents
        
        # ファイルが存在するか確認
        if not Path(self.file_path).exists():
            raise FileNotFoundError(f"Source file '{self.file_path}' not found")
        
        # ファイルから該当行を読み込む
        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # 行番号は1ベース、配列インデックスは0ベース
            original_start_idx = self.line_num_start - 1
            original_end_idx = self.line_num_end if self.line_num_end > 0 else len(lines)
            
            # 実際の開始位置と終了位置を調整
            actual_start_idx = self._find_actual_start(lines, original_start_idx)
            actual_end_idx = self._find_actual_end(lines, original_start_idx, original_end_idx)
            
            # 調整後の行番号（1ベース）
            actual_start_line = actual_start_idx + 1
            actual_end_line = actual_end_idx  # actual_end_idxはexclusiveなので、最終行はactual_end_idx
            
            # ヘッダー情報を作成
            header = f"Source: {self.file_path}:{actual_start_line}-{actual_end_line}\n"
            
            # 該当範囲の行を結合
            source_code = ''.join(lines[actual_start_idx:actual_end_idx])
            
            # ヘッダーとソースコードを結合
            self._contents = header + source_code
            
        except Exception as e:
            raise RuntimeError(f"Error reading source file: {e}")
        
        return self._contents
    
    def _find_actual_start(self, lines: List[str], original_start_idx: int) -> int:
        """
        シンボル定義の実際の開始位置を見つける（コメントや返り値の型を含む）
        
        Args:
            lines: ファイルの全行
            original_start_idx: 元の開始インデックス（0ベース）
            
        Returns:
            int: 実際の開始インデックス
        """
        if original_start_idx == 0:
            return 0
        
        # 現在の位置から遡って探索
        idx = original_start_idx - 1
        actual_start = original_start_idx
        in_comment = False
        comment_start = -1
        
        while idx >= 0:
            line = lines[idx].rstrip()
            
            # ブロックコメントの終了を検出
            if '*/' in line and not in_comment:
                in_comment = True
                comment_start = idx
            
            # ブロックコメントの開始を検出
            if '/*' in line and in_comment:
                actual_start = idx
                in_comment = False
                idx -= 1
                continue
            
            # コメント中の場合はスキップ
            if in_comment:
                idx -= 1
                continue
            
            # 空行または空白のみの行
            if not line or line.isspace():
                idx -= 1
                continue
            
            # 単一行コメント
            if line.strip().startswith('//'):
                actual_start = idx
                idx -= 1
                continue
            
            # プリプロセッサディレクティブ
            if line.strip().startswith('#'):
                # #defineや#ifdefなどは含めない（別のシンボル定義の可能性）
                break
            
            # 関数の返り値の型やstatic/externなどの修飾子
            # セミコロンや開き括弧で終わる行は別の定義の可能性
            if line.endswith(';') or line.endswith('{'):
                break
            
            # typedefやstruct/enum/unionキーワード
            keywords = ['typedef', 'struct', 'enum', 'union', 'static', 'extern', 
                       'const', 'volatile', 'inline', 'register']
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in keywords):
                # 次の行に続いている可能性があるので含める
                actual_start = idx
                idx -= 1
                continue
            
            # その他の場合（変数の型など）
            # アルファベットで始まる行は返り値の型の可能性
            if line and line[0].isalpha():
                actual_start = idx
                idx -= 1
                continue
            
            # それ以外の場合は探索を終了
            break
        
        return actual_start
    
    def _find_actual_end(self, lines: List[str], start_idx: int, original_end_idx: int) -> int:
        """
        シンボル定義の実際の終了位置を見つける（次のシンボルのコメントを除外）
        
        Args:
            lines: ファイルの全行
            start_idx: 開始インデックス（0ベース）
            original_end_idx: 元の終了インデックス（0ベース、exclusive）
            
        Returns:
            int: 実際の終了インデックス
        """
        # デフォルトは元の終了位置
        actual_end = original_end_idx
        
        # シンボルの種類を判定
        if start_idx < len(lines):
            first_line = lines[start_idx].strip()
            
            # マクロ定義の場合
            if first_line.startswith('#define'):
                # 継続行（\で終わる）を追跡
                idx = start_idx
                while idx < original_end_idx and idx < len(lines):
                    line = lines[idx].rstrip()
                    if not line.endswith('\\'):
                        return min(idx + 1, original_end_idx)
                    idx += 1
                return original_end_idx
        
        # 関数や構造体の場合、括弧の対応を追跡
        brace_count = 0
        found_first_brace = False
        idx = start_idx
        
        while idx < original_end_idx and idx < len(lines):
            line = lines[idx]
            
            # 文字列リテラルやコメントを除外した括弧のカウント
            in_string = False
            in_char = False
            in_line_comment = False
            in_block_comment = False
            prev_char = ''
            
            i = 0
            while i < len(line):
                char = line[i]
                
                # 文字列リテラル
                if char == '"' and prev_char != '\\' and not in_char and not in_line_comment and not in_block_comment:
                    in_string = not in_string
                # 文字リテラル
                elif char == "'" and prev_char != '\\' and not in_string and not in_line_comment and not in_block_comment:
                    in_char = not in_char
                # 行コメント
                elif i < len(line) - 1 and line[i:i+2] == '//' and not in_string and not in_char and not in_block_comment:
                    in_line_comment = True
                    i += 1
                # ブロックコメント開始
                elif i < len(line) - 1 and line[i:i+2] == '/*' and not in_string and not in_char and not in_line_comment:
                    in_block_comment = True
                    i += 1
                # ブロックコメント終了
                elif i < len(line) - 1 and line[i:i+2] == '*/' and in_block_comment:
                    in_block_comment = False
                    i += 1
                # 括弧のカウント
                elif not in_string and not in_char and not in_line_comment and not in_block_comment:
                    if char == '{':
                        brace_count += 1
                        found_first_brace = True
                    elif char == '}':
                        brace_count -= 1
                        if found_first_brace and brace_count == 0:
                            # 構造体の場合は}の後のセミコロンまで含める
                            remaining = line[i+1:].strip()
                            if remaining.startswith(';'):
                                return min(idx + 1, original_end_idx)
                            # typedef structの場合、型名の定義まで含める
                            elif remaining and not remaining.startswith('/'):
                                # 次の行も確認
                                if idx + 1 < original_end_idx:
                                    next_line = lines[idx + 1].strip()
                                    if next_line.startswith(';'):
                                        return min(idx + 2, original_end_idx)
                                return min(idx + 1, original_end_idx)
                            else:
                                return min(idx + 1, original_end_idx)
                
                prev_char = char if char != '\\' else prev_char
                i += 1
            
            idx += 1
        
        # 括弧の対応が見つからない場合、次のコメントを除外
        # 終了位置から遡って、最後の非空白・非コメント行を探す
        idx = original_end_idx - 1
        while idx > start_idx:
            line = lines[idx].strip()
            
            # 空行やコメント行でない行が見つかったら、そこまでを含める
            if line and not line.startswith('/*') and not line.startswith('*') and not line.startswith('//'):
                # ブロックコメントの途中でないかチェック
                if '*/' in line:
                    # この行がブロックコメントの終了を含む場合、コメント開始を探す
                    comment_start = idx
                    while comment_start > start_idx:
                        if '/*' in lines[comment_start]:
                            # コメントの開始が見つかったら、その前まで
                            return comment_start
                        comment_start -= 1
                return min(idx + 1, original_end_idx)
            
            idx -= 1
        
        return original_end_idx
    
    def get_references_from_this(self) -> str:
        """
        自身が参照するシンボルを一行ずつ並べた文字列を返す
        line_num_in_fromでソートし、ファイル名と行番号も含める
        
        Returns:
            str: 参照情報の文字列
        """
        conn = self._db.get_connection()
        
        # 自身が参照するシンボルを取得
        results = conn.execute(f"""
            SELECT 
                sr.to_node,
                sr.line_num_in_from,
                sd.symbol_name,
                sd.file_path,
                sd.line_num_start
            FROM {REFERENCE_TABLE} sr
            JOIN {SYMBOL_TABLE} sd ON sr.to_node = sd.id
            WHERE sr.from_node = ?
            ORDER BY sr.line_num_in_from
        """, (self.id,)).fetchall()
        
        if not results:
            return "No references from this symbol"
        
        # 結果を整形
        lines = []
        for to_node, line_num_in_from, symbol_name, file_path, line_num_start in results:
            # ファイル名のみ抽出
            filename = Path(file_path).name
            lines.append(
                f"Line {line_num_in_from:5d}: {symbol_name:30s} "
                f"({filename}:{line_num_start})"
            )
        
        return '\n'.join(lines)
    
    def get_references_to_this(self) -> str:
        """
        自身を参照するシンボルを一行ずつ並べた文字列を返す
        ファイル名と行番号も含める
        
        Returns:
            str: 参照元情報の文字列
        """
        conn = self._db.get_connection()
        
        # 自身を参照するシンボルを取得
        results = conn.execute(f"""
            SELECT 
                sr.from_node,
                sr.line_num_in_from,
                sd.symbol_name,
                sd.file_path,
                sd.line_num_start
            FROM {REFERENCE_TABLE} sr
            JOIN {SYMBOL_TABLE} sd ON sr.from_node = sd.id
            WHERE sr.to_node = ?
            ORDER BY sd.file_path, sr.line_num_in_from
        """, (self.id,)).fetchall()
        
        if not results:
            return "No references to this symbol"
        
        # 結果を整形
        lines = []
        for from_node, line_num_in_from, symbol_name, file_path, line_num_start in results:
            # ファイル名のみ抽出
            filename = Path(file_path).name
            lines.append(
                f"{symbol_name:30s} at line {line_num_in_from:5d} "
                f"({filename}:{line_num_start})"
            )
        
        return '\n'.join(lines)
    
    def __str__(self) -> str:
        """文字列表現"""
        return (f"SNode(id={self.id}, symbol='{self.symbol_name}', "
                f"file='{self.file_path}', lines={self.line_num_start}-{self.line_num_end})")
    
    def __repr__(self) -> str:
        """開発者向け文字列表現"""
        return self.__str__()


# ユーティリティ関数
@lru_cache(maxsize=128)
def get_symbol_names() -> List[str]:
    """
    データベース内の全ユニークシンボル名を取得
    
    Returns:
        List[str]: シンボル名のリスト
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    results = conn.execute(f"""
        SELECT DISTINCT symbol_name 
        FROM {SYMBOL_TABLE}
        ORDER BY symbol_name
    """).fetchall()
    
    return [row[0] for row in results]


def search_symbols(pattern: str) -> List[str]:
    """
    パターンにマッチするシンボル名を検索
    
    Args:
        pattern: 検索パターン（SQL LIKE構文）
        
    Returns:
        List[str]: マッチしたシンボル名のリスト
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    results = conn.execute(f"""
        SELECT DISTINCT symbol_name 
        FROM {SYMBOL_TABLE}
        WHERE symbol_name LIKE ?
        ORDER BY symbol_name
    """, (pattern,)).fetchall()
    
    return [row[0] for row in results]


def get_symbol_by_file_and_line(file_path: str, line_num: int) -> Optional[SNode]:
    """
    ファイルパスと行番号からシンボルを取得
    
    Args:
        file_path: ファイルパス
        line_num: 行番号
        
    Returns:
        SNode: 該当するシンボル、見つからない場合はNone
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    result = conn.execute(f"""
        SELECT id
        FROM {SYMBOL_TABLE}
        WHERE file_path = ?
          AND line_num_start <= ?
          AND (line_num_end >= ? OR line_num_end = 0)
        ORDER BY id
        LIMIT 1
    """, (file_path, line_num, line_num)).fetchone()
    
    if result:
        return SNode.from_id(result[0])
    return None


# テスト用のメイン関数
def main():
    """テスト用のメイン関数"""
    print("SNode Module Test")
    print("=" * 60)
    
    # シンボル名のリストを取得
    symbols = get_symbol_names()
    print(f"Total unique symbols: {len(symbols)}")
    
    # サンプルシンボルを検索
    if symbols:
        # 最初のシンボルでテスト
        test_symbol = symbols[0]
        print(f"\nTesting with symbol: {test_symbol}")
        
        # SNodeオブジェクトを作成
        node = SNode(test_symbol)
        print(f"Created: {node}")
        
        # ソースコードを取得
        try:
            code = node.get_source_code()
            print(f"\nSource code (first 200 chars):")
            print(code[:200] + "..." if len(code) > 200 else code)
        except Exception as e:
            print(f"Error getting source code: {e}")
        
        # 参照情報を表示
        print("\n--- References FROM this symbol ---")
        print(node.get_references_from_this())
        
        print("\n--- References TO this symbol ---")
        print(node.get_references_to_this())
        
        # IDからの作成テスト
        print("\n" + "=" * 60)
        print("Testing factory function from_id...")
        node2 = SNode.from_id(node.id)
        print(f"Created from ID {node.id}: {node2}")


if __name__ == "__main__":
    main()
