#!/usr/bin/env python3
"""
PostgreSQLコードベース用のMCPツール
コマンドライン版 - エンドポイント名と引数を受け取ってJSONを出力
"""

import json
import sys
import argparse
from pathlib import Path

try:
    from snode_module import SNode, search_symbols, DatabaseConnection
except ImportError:
    print(json.dumps({"error": "FATAL: snode_module.py not found. Please place it in the same directory."}))
    sys.exit(1)
except FileNotFoundError as e:
    print(json.dumps({"error": f"FATAL: Database file not found: {e}"}))
    sys.exit(1)

# AIエージェントが生成したドキュメントを一時保存するディレクトリ
TEMP_OUTPUT_DIR = Path("output/temp")


def get_symbol_details(symbol_name: str) -> dict:
    """シンボルの詳細情報を取得"""
    try:
        node = SNode(symbol_name)
        return {
            "id": node.id,
            "symbol_name": node.symbol_name,
            "file_path": node.file_path,
            "start_line": node.line_num_start,
            "end_line": node.line_num_end,
            "type": node.symbol_type
        }
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Failed to get symbol details: {e}"}


def get_symbol_source(symbol_name: str) -> dict:
    """シンボルのソースコードを取得"""
    try:
        node = SNode(symbol_name)
        source_code = node.get_source_code()
        return {"source_code": source_code}
    except ValueError as e:
        return {"error": str(e)}
    except FileNotFoundError as e:
        return {"error": f"Source file not found: {e}"}
    except Exception as e:
        return {"error": f"Failed to get source code: {e}"}


def get_references_from_this(symbol_name: str) -> dict:
    """このシンボルから参照しているシンボルを取得"""
    try:
        node = SNode(symbol_name)
        references = node.get_references_from_this()
        return {"references": references}
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Failed to get references: {e}"}


def get_references_to_this(symbol_name: str) -> dict:
    """このシンボルを参照しているシンボルを取得"""
    try:
        node = SNode(symbol_name)
        referenced_by = node.get_references_to_this()
        return {"referenced_by": referenced_by}
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Failed to get referenced by: {e}"}


def search_symbols_wrapper(pattern: str) -> dict:
    """パターンでシンボルを検索"""
    try:
        symbols = search_symbols(pattern)
        return {"symbols": symbols}
    except Exception as e:
        return {"error": f"Failed to search symbols: {e}"}


def return_document(symbol_name: str, content: str) -> dict:
    """ドキュメントを一時ファイルとして保存"""
    try:
        TEMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        file_path = TEMP_OUTPUT_DIR / f"{symbol_name}.md"
        file_path.write_text(content, encoding='utf-8')
        
        return {
            "status": "success",
            "message": f"Document for '{symbol_name}' saved to {file_path}"
        }
    except IOError as e:
        return {"error": f"Failed to save document: {e}"}


def main():
    parser = argparse.ArgumentParser(
        description='PostgreSQL codebase MCP tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s get_symbol_details heap_insert
  %(prog)s get_symbol_source heap_insert
  %(prog)s get_references_from_this heap_insert
  %(prog)s get_references_to_this palloc
  %(prog)s search_symbols "heap_*"
  %(prog)s save_document heap_insert "# heap_insert\\n\\nInserts a tuple..."
        """
    )
    
    parser.add_argument(
        'method',
        choices=[
            'get_symbol_details',
            'get_symbol_source',
            'get_references_from_this',
            'get_references_to_this',
            'search_symbols',
            'return_document'
        ],
        help='Method to call'
    )
    
    parser.add_argument(
        'args',
        nargs='*',
        help='Arguments for the method'
    )
    
    # JSON入力オプション（複雑な引数用）
    parser.add_argument(
        '--json-args',
        type=str,
        help='Arguments as JSON string (alternative to positional args)'
    )
    
    args = parser.parse_args()
    
    try:
        # 引数の処理
        if args.json_args:
            # JSON文字列から引数を解析
            params = json.loads(args.json_args)
        else:
            # 位置引数から適切なパラメータを構築
            if args.method == 'return_document':
                if len(args.args) < 2:
                    print(json.dumps({"error": "return_document requires 2 arguments: symbol_name and content"}))
                    sys.exit(1)
                params = {
                    'symbol_name': args.args[0],
                    'content': ' '.join(args.args[1:])  # 残りの引数をcontentとして結合
                }
            else:
                if not args.args:
                    print(json.dumps({"error": f"{args.method} requires at least 1 argument"}))
                    sys.exit(1)
                # 他のメソッドは最初の引数のみを使用
                params = args.args[0]
        
        # メソッドの実行
        if args.method == 'get_symbol_details':
            if isinstance(params, dict):
                result = get_symbol_details(params.get('symbol_name', ''))
            else:
                result = get_symbol_details(params)
                
        elif args.method == 'get_symbol_source':
            if isinstance(params, dict):
                result = get_symbol_source(params.get('symbol_name', ''))
            else:
                result = get_symbol_source(params)
                
        elif args.method == 'get_references_from_this':
            if isinstance(params, dict):
                result = get_references_from_this(params.get('symbol_name', ''))
            else:
                result = get_references_from_this(params)
                
        elif args.method == 'get_references_to_this':
            if isinstance(params, dict):
                result = get_references_to_this(params.get('symbol_name', ''))
            else:
                result = get_references_to_this(params)
                
        elif args.method == 'search_symbols':
            if isinstance(params, dict):
                result = search_symbols_wrapper(params.get('pattern', ''))
            else:
                result = search_symbols_wrapper(params)
                
        elif args.method == 'return_document':
            if isinstance(params, dict):
                result = return_document(
                    params.get('symbol_name', ''),
                    params.get('content', '')
                )
            else:
                print(json.dumps({"error": "return_document requires dict params"}))
                sys.exit(1)
        
        # 結果をJSONで出力
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON format: {e}"}))
        sys.exit(1)
    except KeyboardInterrupt:
        print(json.dumps({"error": "Interrupted by user"}))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": f"Unexpected error: {e}"}))
        sys.exit(1)
    finally:
        # データベース接続をクリーンアップ
        try:
            DatabaseConnection().close()
        except:
            pass


if __name__ == '__main__':
    main()