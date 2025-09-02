#!/usr/bin/env python3
"""
CSVファイルの第2要素（2番目のカラム）について、
同一値の行数を集計し、多い順に上位40位を表示するスクリプト
--excludeオプションで上位40位の値を含む行を除外してから再集計可能
--excludeオプション使用時は除外後のCSVデータを自動的に出力
"""

import csv
import argparse
from collections import Counter
from typing import List, Optional


def get_top_values_from_csv(filepath: str, top_n: int = 40) -> List[str]:
    """
    CSVファイルの第2要素を集計し、上位N位の値のリストを返す
    
    Args:
        filepath: CSVファイルのパス
        top_n: 上位何位まで取得するか
        
    Returns:
        上位N位の値のリスト
    """
    second_elements = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # 空行をスキップ
                if not row:
                    continue
                    
                # 2つ以上の要素があることを確認
                if len(row) < 2:
                    continue
                
                second_element = row[1].strip()  # 空白を除去
                second_elements.append(second_element)
                    
    except FileNotFoundError:
        print(f"エラー: ファイル '{filepath}' が見つかりません")
        return []
    except Exception as e:
        print(f"エラー: ファイル読み込み中にエラーが発生しました: {e}")
        return []
    
    # 出現回数をカウントして上位N位の値を取得
    counter = Counter(second_elements)
    top_items = counter.most_common(top_n)
    
    return [value for value, count in top_items]


def filter_csv_excluding_top_values(
    input_filepath: str,
    output_filepath: str,
    exclude_top40: bool = False
) -> int:
    """
    上位40位の値を含む行を除外したCSVファイルを出力する
    
    Args:
        input_filepath: 入力CSVファイルのパス
        output_filepath: 出力CSVファイルのパス
        exclude_top40: 上位40位の値を除外するかどうか
        
    Returns:
        出力された行数
    """
    # 除外する値のリストを取得
    exclude_values = []
    if exclude_top40:
        exclude_values = get_top_values_from_csv(input_filepath, 40)
        if not exclude_values:
            return 0
    
    filtered_rows = []
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # 空行をスキップ
                if not row:
                    continue
                    
                # 2つ以上の要素があることを確認
                if len(row) < 2:
                    print(f"警告: 行{row_num}に十分な要素がありません: {row}")
                    continue
                
                second_element = row[1].strip()  # 空白を除去
                
                # 除外リストに含まれていない場合のみ追加
                if not exclude_top40 or second_element not in exclude_values:
                    filtered_rows.append(row)
                    
    except FileNotFoundError:
        print(f"エラー: ファイル '{input_filepath}' が見つかりません")
        return 0
    except Exception as e:
        print(f"エラー: ファイル読み込み中にエラーが発生しました: {e}")
        return 0
    
    # フィルター後のデータを出力
    try:
        with open(output_filepath, 'w', encoding='utf-8', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(filtered_rows)
            
    except Exception as e:
        print(f"エラー: ファイル出力中にエラーが発生しました: {e}")
        return 0
    
    return len(filtered_rows)


def analyze_csv_second_column(
    filepath: str, 
    exclude_top40: bool = False,
    top_n: int = 40
) -> List[tuple]:
    """
    CSVファイルの第2要素を集計し、出現回数の多い順に返す
    
    Args:
        filepath: CSVファイルのパス
        exclude_top40: 上位40位の値を除外するかどうか
        top_n: 上位何位まで表示するか
        
    Returns:
        (値, 出現回数)のタプルのリスト
    """
    # 除外する値のリストを取得
    exclude_values = []
    if exclude_top40:
        exclude_values = get_top_values_from_csv(filepath, 40)
        if not exclude_values:
            return []
    
    # 第2要素を格納するリスト
    second_elements = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # 空行をスキップ
                if not row:
                    continue
                    
                # 2つ以上の要素があることを確認
                if len(row) < 2:
                    print(f"警告: 行{row_num}に十分な要素がありません: {row}")
                    continue
                
                second_element = row[1].strip()  # 空白を除去
                
                # 除外リストに含まれていない場合のみ追加
                if second_element not in exclude_values:
                    second_elements.append(second_element)
                    
    except FileNotFoundError:
        print(f"エラー: ファイル '{filepath}' が見つかりません")
        return []
    except Exception as e:
        print(f"エラー: ファイル読み込み中にエラーが発生しました: {e}")
        return []
    
    # 出現回数をカウント
    counter = Counter(second_elements)
    
    # 多い順にソートして上位N位を取得
    top_items = counter.most_common(top_n)
    
    return top_items


def main():
    parser = argparse.ArgumentParser(
        description='CSVファイルの第2要素の出現回数を集計します'
    )
    
    parser.add_argument(
        'filepath',
        help='CSVファイルのパス'
    )
    
    parser.add_argument(
        '-e', '--exclude',
        action='store_true',
        help='上位40位の値を含む行を除外してから再集計する'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='除外後のCSVデータの出力ファイルパス（--excludeと組み合わせて使用）'
    )
    
    parser.add_argument(
        '-n', '--top',
        type=int,
        default=40,
        help='表示する上位N位（デフォルト: 40）'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='詳細情報を表示'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"ファイル: {args.filepath}")
        if args.exclude:
            print("除外モード: 上位40位の値を除外してから再集計")
            if args.output:
                print(f"除外後CSVファイル出力: {args.output}")
        print(f"表示件数: {args.top}")
        print("-" * 50)
    
    # 除外後のCSVファイル出力（--excludeと--outputが指定された場合）
    if args.exclude and args.output:
        output_rows = filter_csv_excluding_top_values(
            args.filepath,
            args.output,
            True
        )
        
        if output_rows > 0:
            print(f"除外後のCSVデータを '{args.output}' に出力しました ({output_rows} 行)")
            print("-" * 50)
        else:
            print("CSVデータの出力に失敗しました")
            return
    elif args.exclude and not args.output:
        # --excludeが指定されているが--outputが指定されていない場合
        # 自動的に出力ファイル名を生成
        input_name = args.filepath.rsplit('.', 1)[0]  # 拡張子を除去
        auto_output = f"{input_name}_filtered.csv"
        
        output_rows = filter_csv_excluding_top_values(
            args.filepath,
            auto_output,
            True
        )
        
        if output_rows > 0:
            print(f"除外後のCSVデータを '{auto_output}' に出力しました ({output_rows} 行)")
            print("-" * 50)
        else:
            print("CSVデータの出力に失敗しました")
            return
    
    # 分析実行
    results = analyze_csv_second_column(
        args.filepath,
        args.exclude,
        args.top
    )
    
    if not results:
        print("結果が得られませんでした")
        return
    
    # 結果表示
    if args.exclude:
        print(f"第2要素の出現回数 上位{min(len(results), args.top)}位 (上位40位除外後):")
    else:
        print(f"第2要素の出現回数 上位{min(len(results), args.top)}位:")
    print("-" * 50)
    print(f"{'順位':<4} {'値':<15} {'出現回数':<8}")
    print("-" * 50)
    
    for rank, (value, count) in enumerate(results, 1):
        print(f"{rank:<4} {value:<15} {count:<8}")
    
    if args.verbose:
        total_rows = sum(count for _, count in results)
        print("-" * 50)
        print(f"集計対象行数: {total_rows}")


if __name__ == "__main__":
    main()
