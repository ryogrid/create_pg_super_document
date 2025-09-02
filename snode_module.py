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
            self._connection = duckdb.connect(DB_FILE, read_only=True)
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
            SELECT id, file_path, line_num_start, line_num_end
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
        
        Returns:
            str: ソースコード
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
            start_idx = self.line_num_start - 1
            
            # line_num_endが0の場合はファイル末尾まで
            if self.line_num_end == 0:
                end_idx = len(lines)
            else:
                end_idx = self.line_num_end
            
            # 該当範囲の行を結合
            self._contents = ''.join(lines[start_idx:end_idx])
            
        except Exception as e:
            raise RuntimeError(f"Error reading source file: {e}")
        
        return self._contents
    
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
