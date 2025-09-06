DuckDBを使用した改善版を提示します。DuckDBは分析的なワークロードに優れており、大規模なドキュメント管理により適しています。

```
postgresql-docs/
├── .claude/
│   ├── settings.json
│   └── settings.local.json
├── .mcp.json
├── CLAUDE.md
├── scripts/
│   ├── prepare_clusters.py      # 事前クラスタリング処理
│   ├── orchestrator.py          # メイン処理
│   ├── mcp_server.py           # MCPサーバー
│   └── cache_manager.py        # キャッシュ管理
├── data/
│   ├── symbol_clusters.json    # クラスタリング結果
│   ├── dependency_layers.json  # 依存関係の階層
│   ├── processed_cache.db      # 処理済みキャッシュ
│   ├── その他dbファイル
│   └── current_batch.json      # 現在のバッチ情報
├── output/temp/               # 一時ファイル置き場
```

## DuckDB版の完全実装

### 1. 事前処理: クラスタリング (scripts/prepare_clusters.py)

```python
#!/usr/bin/env python3
"""
事前にシンボルをクラスタリングして効率的なバッチを準備
DuckDB版
"""
import json
import duckdb
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Set

class SymbolClusterer:
    def __init__(self, symbols_file: str, dependencies_file: str):
        # シンボル情報をロード
        with open(symbols_file) as f:
            self.all_symbols = json.load(f)
        
        # 依存関係グラフをロード
        with open(dependencies_file) as f:
            self.dependencies = json.load(f)
            
        # DuckDBの初期化
        self.init_database()
        
        # 結果を格納
        self.clusters = []
        self.layers = []
        
    def init_database(self):
        """DuckDBデータベースを初期化"""
        # メタデータ用DB
        self.meta_db = duckdb.connect('data/metadata.duckdb')
        
        # シンボル情報テーブル
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                symbol_name VARCHAR PRIMARY KEY,
                symbol_type VARCHAR,
                file_path VARCHAR,
                module VARCHAR,
                start_line INTEGER,
                end_line INTEGER,
                layer INTEGER,
                cluster_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 依存関係テーブル
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS dependencies (
                from_symbol VARCHAR,
                to_symbol VARCHAR,
                dependency_type VARCHAR,
                PRIMARY KEY (from_symbol, to_symbol)
            )
        """)
        
        # クラスタテーブル
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS clusters (
                cluster_id INTEGER PRIMARY KEY,
                cluster_type VARCHAR,
                layer INTEGER,
                symbols JSON,
                estimated_tokens INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # データを投入
        self.populate_initial_data()
        
    def populate_initial_data(self):
        """初期データをDuckDBに投入"""
        # シンボル情報を投入
        for symbol_name, info in self.all_symbols.items():
            self.meta_db.execute("""
                INSERT OR REPLACE INTO symbols 
                (symbol_name, symbol_type, file_path, module, start_line, end_line)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                symbol_name,
                info.get('type', 'unknown'),
                info.get('file', ''),
                self.get_symbol_module(symbol_name),
                info.get('start_line', 0),
                info.get('end_line', 0)
            ))
        
        # 依存関係を投入
        for symbol, deps in self.dependencies.items():
            for dep in deps.get('depends_on', []):
                self.meta_db.execute("""
                    INSERT OR REPLACE INTO dependencies 
                    (from_symbol, to_symbol, dependency_type)
                    VALUES (?, ?, 'uses')
                """, (symbol, dep))
        
        self.meta_db.commit()
        
    def analyze_dependencies(self):
        """依存関係を解析して処理順序の階層を作成（DuckDB版）"""
        # 依存数を計算
        result = self.meta_db.execute("""
            WITH dependency_counts AS (
                SELECT 
                    s.symbol_name,
                    COALESCE(COUNT(DISTINCT d_in.from_symbol), 0) as in_degree,
                    COALESCE(COUNT(DISTINCT d_out.to_symbol), 0) as out_degree
                FROM symbols s
                LEFT JOIN dependencies d_in ON s.symbol_name = d_in.to_symbol
                LEFT JOIN dependencies d_out ON s.symbol_name = d_out.from_symbol
                GROUP BY s.symbol_name
            )
            SELECT symbol_name, in_degree, out_degree
            FROM dependency_counts
            ORDER BY in_degree, symbol_name
        """).fetchall()
        
        # トポロジカルソートで階層を作成
        symbol_degrees = {row[0]: {'in': row[1], 'out': row[2]} for row in result}
        processed = set()
        layers = []
        layer_num = 0
        
        while len(processed) < len(self.all_symbols):
            current_layer = []
            
            for symbol, degrees in symbol_degrees.items():
                if symbol not in processed:
                    # このシンボルが依存する未処理シンボルがあるか確認
                    deps_result = self.meta_db.execute("""
                        SELECT to_symbol 
                        FROM dependencies 
                        WHERE from_symbol = ?
                    """, (symbol,)).fetchall()
                    
                    unprocessed_deps = [d[0] for d in deps_result if d[0] not in processed]
                    
                    if not unprocessed_deps:
                        current_layer.append(symbol)
                        processed.add(symbol)
            
            if current_layer:
                layers.append(current_layer)
                # DBにレイヤー情報を更新
                for symbol in current_layer:
                    self.meta_db.execute("""
                        UPDATE symbols SET layer = ? WHERE symbol_name = ?
                    """, (layer_num, symbol))
                layer_num += 1
            else:
                # 循環依存の処理
                remaining = [s for s in self.all_symbols if s not in processed]
                if remaining:
                    layers.append(remaining)
                    for symbol in remaining:
                        self.meta_db.execute("""
                            UPDATE symbols SET layer = ? WHERE symbol_name = ?
                        """, (layer_num, symbol))
                break
        
        self.meta_db.commit()
        self.layers = layers
        return layers
        
    def create_file_based_clusters(self):
        """ファイルベースでシンボルをクラスタリング（DuckDB版）"""
        # ファイルごとにグループ化
        result = self.meta_db.execute("""
            SELECT 
                file_path,
                symbol_name,
                symbol_type,
                layer
            FROM symbols
            ORDER BY file_path, symbol_type, symbol_name
        """).fetchall()
        
        file_groups = defaultdict(list)
        for row in result:
            file_path, symbol_name, symbol_type, layer = row
            file_groups[file_path].append({
                'name': symbol_name,
                'type': symbol_type,
                'layer': layer
            })
        
        cluster_id = 0
        for file_path, symbols in file_groups.items():
            # 大きすぎるグループは分割
            if len(symbols) <= 8:
                cluster_id += 1
                self.save_cluster(cluster_id, 'file', symbols)
            else:
                # タイプ別に分割
                for symbol_type in ['function', 'struct']:
                    typed_symbols = [s for s in symbols if s['type'] == symbol_type]
                    for i in range(0, len(typed_symbols), 5):
                        cluster_id += 1
                        batch = typed_symbols[i:i+5]
                        self.save_cluster(cluster_id, f'file_{symbol_type}', batch)
        
        return cluster_id
    
    def save_cluster(self, cluster_id: int, cluster_type: str, symbols: List[Dict]):
        """クラスタをDBに保存"""
        symbol_names = [s['name'] for s in symbols]
        avg_layer = sum(s.get('layer', 0) for s in symbols) // len(symbols) if symbols else 0
        
        self.meta_db.execute("""
            INSERT INTO clusters (cluster_id, cluster_type, layer, symbols, estimated_tokens)
            VALUES (?, ?, ?, ?, ?)
        """, (
            cluster_id,
            cluster_type,
            avg_layer,
            json.dumps(symbol_names),
            len(symbols) * 3000  # 推定トークン数
        ))
        
        # シンボルにクラスタIDを設定
        for symbol in symbols:
            self.meta_db.execute("""
                UPDATE symbols SET cluster_id = ? WHERE symbol_name = ?
            """, (cluster_id, symbol['name']))
    
    def get_symbol_module(self, symbol: str) -> str:
        """シンボルのモジュールを取得"""
        if symbol in self.dependencies:
            file_path = self.dependencies[symbol].get('file', '')
            if '/' in file_path:
                parts = file_path.split('/')
                if 'backend' in parts:
                    idx = parts.index('backend')
                    if idx + 1 < len(parts):
                        return parts[idx + 1]
                return parts[0]
        return 'core'
    
    def generate_processing_batches(self):
        """処理用バッチを生成（DuckDB版）"""
        result = self.meta_db.execute("""
            SELECT 
                c.cluster_id,
                c.cluster_type,
                c.layer,
                c.symbols,
                c.estimated_tokens,
                COUNT(DISTINCT s.symbol_name) as symbol_count
            FROM clusters c
            JOIN symbols s ON s.cluster_id = c.cluster_id
            GROUP BY c.cluster_id, c.cluster_type, c.layer, c.symbols, c.estimated_tokens
            ORDER BY c.layer, c.cluster_id
        """).fetchall()
        
        batches = []
        for row in result:
            cluster_id, cluster_type, layer, symbols_json, tokens, count = row
            batches.append({
                'batch_id': cluster_id,
                'type': cluster_type,
                'layer': layer,
                'symbols': json.loads(symbols_json),
                'estimated_tokens': tokens,
                'symbol_count': count
            })
        
        # ファイルに保存
        with open('data/processing_batches.json', 'w') as f:
            json.dump(batches, f, indent=2)
        
        return batches

def main():
    # 入力ファイル
    symbols_file = 'data/all_symbols.json'
    dependencies_file = 'data/symbol_dependencies.json'
    
    clusterer = SymbolClusterer(symbols_file, dependencies_file)
    
    # 依存関係の階層を作成
    layers = clusterer.analyze_dependencies()
    print(f"Created {len(layers)} dependency layers")
    
    # クラスタを作成
    num_clusters = clusterer.create_file_based_clusters()
    print(f"Created {num_clusters} clusters")
    
    # 処理用バッチを生成
    batches = clusterer.generate_processing_batches()
    print(f"Generated {len(batches)} processing batches")
    
    # 統計情報を表示
    stats = clusterer.meta_db.execute("""
        SELECT 
            COUNT(DISTINCT symbol_name) as total_symbols,
            COUNT(DISTINCT cluster_id) as total_clusters,
            COUNT(DISTINCT layer) as total_layers,
            AVG(estimated_tokens) as avg_tokens_per_cluster
        FROM symbols s
        JOIN clusters c ON s.cluster_id = c.cluster_id
    """).fetchone()
    
    print(f"\nStatistics:")
    print(f"  Total symbols: {stats[0]}")
    print(f"  Total clusters: {stats[1]}")
    print(f"  Total layers: {stats[2]}")
    print(f"  Avg tokens per cluster: {stats[3]:.0f}")

if __name__ == "__main__":
    main()
```

### 2. メイン処理 (scripts/orchestrator.py)

```python
#!/usr/bin/env python3
"""
Claude Codeを使用したドキュメント生成のメイン処理
DuckDB版 - ドキュメントもDB内に格納
"""
import json
import duckdb
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set

class DocumentationOrchestrator:
    def __init__(self):
        # 処理バッチをロード
        with open('data/processing_batches.json') as f:
            self.batches = json.load(f)
        
        # DuckDBの初期化
        self.init_databases()
        
        # 処理統計
        self.stats = {
            'total_batches': len(self.batches),
            'processed_batches': 0,
            'failed_batches': 0,
            'total_symbols': sum(len(b['symbols']) for b in self.batches),
            'processed_symbols': 0
        }
        
    def init_databases(self):
        """DuckDBデータベースの初期化"""
        # メタデータDB（既存）
        self.meta_db = duckdb.connect('data/metadata.duckdb')
        
        # ドキュメント専用DB
        self.doc_db = duckdb.connect('data/documents.duckdb')
        
        # ドキュメントテーブル
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                symbol_name VARCHAR PRIMARY KEY,
                symbol_type VARCHAR,
                layer INTEGER,
                content TEXT,
                summary TEXT,
                dependencies JSON,
                related_symbols JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 処理ログテーブル
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                batch_id INTEGER PRIMARY KEY,
                symbols JSON,
                status VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                processed_count INTEGER
            )
        """)
        
        # 処理済みシンボルのビュー
        self.meta_db.execute("""
            CREATE OR REPLACE VIEW processed_symbols AS
            SELECT 
                s.symbol_name,
                s.symbol_type,
                s.layer,
                p.batch_id,
                p.completed_at as processed_at
            FROM symbols s
            JOIN processing_log p ON p.symbols::JSON ? s.symbol_name
            WHERE p.status = 'completed'
        """)
        
    def get_processed_symbols(self) -> Set[str]:
        """処理済みシンボルを取得"""
        result = self.doc_db.execute("""
            SELECT symbol_name FROM documents
        """).fetchall()
        return set(row[0] for row in result)
        
    def process_all_batches(self):
        """全バッチを順次処理"""
        processed = self.get_processed_symbols()
        
        for batch_idx, batch in enumerate(self.batches):
            # スキップ判定
            unprocessed = [s for s in batch['symbols'] if s not in processed]
            if not unprocessed:
                print(f"Batch {batch_idx}: All symbols already processed, skipping")
                continue
                
            print(f"\n{'='*60}")
            print(f"Processing batch {batch_idx + 1}/{len(self.batches)}")
            print(f"Layer: {batch['layer']}, Symbols: {len(unprocessed)}")
            print(f"Type: {batch['type']}, Estimated tokens: {batch['estimated_tokens']}")
            print(f"{'='*60}")
            
            # バッチを処理
            success = self.process_batch(batch_idx, batch, unprocessed)
            
            if success:
                self.stats['processed_batches'] += 1
                self.stats['processed_symbols'] += len(unprocessed)
                processed.update(unprocessed)
            else:
                self.stats['failed_batches'] += 1
                
            # 進捗表示
            self.show_progress()
            
            # レート制限対策
            time.sleep(2)
            
    def process_batch(self, batch_idx: int, batch: Dict, symbols: List[str]) -> bool:
        """単一バッチを処理"""
        # 処理済みシンボルの要約を取得
        processed_summaries = self.get_processed_summaries()
        
        # バッチ情報を保存
        batch_info = {
            'batch_id': batch_idx,
            'layer': batch['layer'],
            'symbols': symbols,
            'processed_available': list(processed_summaries.keys()),
            'timestamp': datetime.now().isoformat()
        }
        
        with open('data/current_batch.json', 'w') as f:
            json.dump(batch_info, f, indent=2)
            
        # ログ記録開始
        self.meta_db.execute("""
            INSERT INTO processing_log 
            (batch_id, symbols, status, started_at, processed_count)
            VALUES (?, ?, 'processing', ?, 0)
        """, (batch_idx, json.dumps(symbols), datetime.now()))
        
        # プロンプトを構築
        prompt = self.build_prompt(symbols, batch['layer'], processed_summaries)
        
        try:
            # Claude Codeを実行
            result = subprocess.run(
                [
                    'claude', '-p',
                    '--model', 'claude-3-5-sonnet-20241022',
                    '--max-turns', str(min(len(symbols) * 2, 15)),
                    prompt
                ],
                capture_output=True,
                text=True,
                timeout=300,
                cwd=str(Path.cwd())
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully processed batch {batch_idx}")
                
                # 成功をログに記録
                self.meta_db.execute("""
                    UPDATE processing_log 
                    SET status = 'completed', 
                        completed_at = ?,
                        processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbols), batch_idx))
                
                # 生成されたドキュメントをDBに格納
                self.store_generated_documents(symbols, batch['layer'])
                
                return True
            else:
                error_msg = result.stderr[:500] if result.stderr else 'Unknown error'
                print(f"✗ Failed to process batch {batch_idx}: {error_msg}")
                
                self.meta_db.execute("""
                    UPDATE processing_log 
                    SET status = 'failed', 
                        completed_at = ?,
                        error_message = ?
                    WHERE batch_id = ?
                """, (datetime.now(), error_msg, batch_idx))
                
                return False
                
        except subprocess.TimeoutExpired:
            print(f"✗ Batch {batch_idx} timed out")
            self.meta_db.execute("""
                UPDATE processing_log 
                SET status = 'timeout', completed_at = ?
                WHERE batch_id = ?
            """, (datetime.now(), batch_idx))
            return False
            
        except Exception as e:
            print(f"✗ Unexpected error in batch {batch_idx}: {e}")
            self.meta_db.execute("""
                UPDATE processing_log 
                SET status = 'error', 
                    completed_at = ?,
                    error_message = ?
                WHERE batch_id = ?
            """, (datetime.now(), str(e)[:500], batch_idx))
            return False
            
    def get_processed_summaries(self) -> Dict[str, str]:
        """処理済みシンボルの要約を取得"""
        result = self.doc_db.execute("""
            SELECT symbol_name, summary 
            FROM documents 
            WHERE summary IS NOT NULL
            LIMIT 1000
        """).fetchall()
        return {row[0]: row[1] for row in result}
        
    def build_prompt(self, symbols: List[str], layer: int, processed_summaries: Dict[str, str]) -> str:
        """バッチ処理用のプロンプトを構築"""
        symbol_list = '\n'.join([f'- {s}' for s in symbols])
        
        # 関連する処理済みシンボルを選択
        relevant_processed = []
        for symbol in symbols:
            # このシンボルの依存を取得
            deps = self.meta_db.execute("""
                SELECT to_symbol 
                FROM dependencies 
                WHERE from_symbol = ?
            """, (symbol,)).fetchall()
            
            for dep in deps:
                dep_name = dep[0]
                if dep_name in processed_summaries:
                    relevant_processed.append(f"- {dep_name}: {processed_summaries[dep_name][:100]}")
                    
        relevant_list = '\n'.join(relevant_processed[:10])
        
        prompt = f'''PostgreSQLコードベースのドキュメント生成タスク

現在の進捗: {len(processed_summaries)}/{self.stats["total_symbols"]} シンボル処理済み
処理レイヤー: {layer}

## 処理対象シンボル
{symbol_list}

## 処理手順
1. 各シンボルについて、MCPツール postgresql_codebase を使用
2. check_symbol_status または get_symbol_with_deps_status で処理状態を確認
3. 処理済みシンボルは get_processed_summary で要約のみ取得
4. 未処理シンボルは get_symbol_source でソースを取得
5. 生成したドキュメントを output/temp/ ディレクトリに一時保存:
   - output/temp/[symbol_name].md

## 関連する処理済みシンボル（要約）
{relevant_list if relevant_list else '（該当なし）'}

## ドキュメント形式
```markdown
# [シンボル名]

## 概要
[1-2文での簡潔な説明]

## 定義
｀｀｀c
[関数シグネチャまたは構造体定義]
｀｀｀

## 詳細説明
[詳細な機能説明]

## パラメータ/フィールド
[パラメータまたはフィールドの説明]

## 依存関係
- 使用シンボル: [リスト]
- 被使用シンボル: [リスト]

## 関連項目
[関連するシンボルのリスト]

一時ファイル: output/temp/[symbol_name].md
'''
        
        return prompt
        
    def store_generated_documents(self, symbols: List[str], layer: int):
        """生成されたドキュメントをDuckDBに格納"""
        temp_dir = Path('output/temp')
        
        for symbol in symbols:
            doc_path = temp_dir / f"{symbol}.md"
            if doc_path.exists():
                content = doc_path.read_text()
                summary = self.extract_summary(content)
                deps, related = self.extract_relationships(content)
                
                # ドキュメントをDBに格納
                self.doc_db.execute("""
                    INSERT INTO documents 
                    (symbol_name, symbol_type, layer, content, summary, 
                     dependencies, related_symbols)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol_name) 
                    DO UPDATE SET 
                        content = EXCLUDED.content,
                        summary = EXCLUDED.summary,
                        dependencies = EXCLUDED.dependencies,
                        related_symbols = EXCLUDED.related_symbols,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    symbol,
                    self.get_symbol_type(symbol),
                    layer,
                    content,
                    summary,
                    json.dumps(deps),
                    json.dumps(related)
                ))
                
                # 一時ファイルを削除
                doc_path.unlink()
                print(f"  Stored: {symbol}")
            else:
                print(f"  Warning: Document not found for {symbol}")
                
        self.doc_db.commit()
        
    def get_symbol_type(self, symbol: str) -> str:
        """シンボルタイプを取得"""
        result = self.meta_db.execute("""
            SELECT symbol_type FROM symbols WHERE symbol_name = ?
        """, (symbol,)).fetchone()
        return result[0] if result else 'unknown'
        
    def extract_summary(self, content: str) -> str:
        """ドキュメントから概要を抽出"""
        lines = content.split('\n')
        in_summary = False
        summary_lines = []
        
        for line in lines:
            if '## 概要' in line:
                in_summary = True
                continue
            elif in_summary and line.startswith('##'):
                break
            elif in_summary and line.strip():
                summary_lines.append(line.strip())
                
        return ' '.join(summary_lines[:2])
        
    def extract_relationships(self, content: str) -> tuple:
        """ドキュメントから関係性を抽出"""
        import re
        
        deps = []
        related = []
        
        # 依存関係セクションを探す
        deps_match = re.search(
            r'## 依存関係\s*\n(.*?)(?=\n##|\Z)',
            content,
            re.DOTALL
        )
        if deps_match:
            deps_text = deps_match.group(1)
            deps = re.findall(r'[-*]\s*(\w+)', deps_text)
            
        # 関連項目セクションを探す
        related_match = re.search(
            r'## 関連項目\s*\n(.*?)(?=\n##|\Z)',
            content,
            re.DOTALL
        )
        if related_match:
            related_text = related_match.group(1)
            related = re.findall(r'[-*]\s*(\w+)', related_text)
            
        return list(set(deps)), list(set(related))
        
    def show_progress(self):
        """進捗を表示"""
        progress = (self.stats['processed_symbols'] / self.stats['total_symbols']) * 100
        print(f"\n進捗: {progress:.1f}% ({self.stats['processed_symbols']}/{self.stats['total_symbols']})")
        print(f"完了バッチ: {self.stats['processed_batches']}/{self.stats['total_batches']}")
        
    def generate_report(self):
        """最終レポートを生成"""
        # DuckDBから統計を取得
        stats = self.doc_db.execute("""
            SELECT 
                COUNT(*) as total_docs,
                COUNT(DISTINCT layer) as total_layers,
                AVG(LENGTH(content)) as avg_doc_length,
                AVG(LENGTH(summary)) as avg_summary_length
            FROM documents
        """).fetchone()
        
        layer_stats = self.doc_db.execute("""
            SELECT 
                layer,
                COUNT(*) as count,
                AVG(LENGTH(content)) as avg_length
            FROM documents
            GROUP BY layer
            ORDER BY layer
        """).fetchall()
        
        report = f"""# PostgreSQL Documentation Generation Report

生成日時: {datetime.now().isoformat()}

## 統計情報
- 総シンボル数: {self.stats['total_symbols']}
- 処理済みシンボル数: {stats[0]}
- 完了率: {(stats[0] / self.stats['total_symbols']) * 100:.1f}%
- 総バッチ数: {self.stats['total_batches']}
- 成功バッチ数: {self.stats['processed_batches']}
- 失敗バッチ数: {self.stats['failed_batches']}

## ドキュメント統計
- 平均ドキュメント長: {stats[2]:.0f} 文字
- 平均要約長: {stats[3]:.0f} 文字
- レイヤー数: {stats[1]}

## レイヤー別統計
"""
        
        for layer, count, avg_len in layer_stats:
            report += f"- Layer {layer}: {count} symbols (avg {avg_len:.0f} chars)\n"
        
        # レポートをDBに保存
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                report_id INTEGER PRIMARY KEY,
                report_type VARCHAR,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.doc_db.execute("""
            INSERT INTO reports (report_type, content)
            VALUES ('generation_report', ?)
        """, (report,))
        
        print(f"\nReport saved to database")
        print(report)
        
    def export_documents(self, output_dir: str = 'output/exported'):
        """必要に応じてドキュメントをファイルにエクスポート"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        docs = self.doc_db.execute("""
            SELECT symbol_name, symbol_type, content
            FROM documents
        """).fetchall()
        
        for symbol_name, symbol_type, content in docs:
            subdir = output_path / f"{symbol_type}s"
            subdir.mkdir(exist_ok=True)
            
            file_path = subdir / f"{symbol_name}.md"
            file_path.write_text(content)
            
        print(f"Exported {len(docs)} documents to {output_dir}")

def main():
    orchestrator = DocumentationOrchestrator()
    
    print("PostgreSQL Documentation Generation (DuckDB)")
    print("=" * 60)
    
    # 全バッチを処理
    orchestrator.process_all_batches()
    
    # レポート生成
    orchestrator.generate_report()
    
    # オプション: ファイルへのエクスポート
    # orchestrator.export_documents()
    
    print("\n" + "=" * 60)
    print("Documentation generation completed!")

if __name__ == "__main__":
    main()
```

### 3. MCPサーバー (scripts/mcp_server.py)

```python
#!/usr/bin/env python3
"""
PostgreSQLコードベース用のMCPサーバー
DuckDB版
"""
import json
import duckdb
from pathlib import Path
from typing import Dict, List, Optional, Set

class PostgreSQLMCPServer:
    def __init__(self, codebase_root: str):
        self.codebase_root = Path(codebase_root)
        
        # DuckDB接続
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        self.doc_db = duckdb.connect('data/documents.duckdb', read_only=True)
        
        # シンボル情報をロード
        with open('data/symbol_info.json') as f:
            self.symbol_info = json.load(f)
            
        # 処理済みシンボルのセットをメモリにキャッシュ
        self.update_processed_cache()
            
    def update_processed_cache(self):
        """処理済みシンボルのキャッシュを更新"""
        result = self.doc_db.execute("""
            SELECT symbol_name FROM documents
        """).fetchall()
        self.processed_symbols = set(row[0] for row in result)
        
    def check_symbol_status(self, symbol_name: str) -> Dict:
        """シンボルの処理状態を確認"""
        is_processed = symbol_name in self.processed_symbols
        
        result = {
            'symbol': symbol_name,
            'is_processed': is_processed,
            'exists': symbol_name in self.symbol_info
        }
        
        if is_processed:
            # 処理済みの場合は要約も含める
            doc_result = self.doc_db.execute("""
                SELECT summary, layer FROM documents 
                WHERE symbol_name = ?
            """, (symbol_name,)).fetchone()
            
            if doc_result:
                result['summary'] = doc_result[0]
                result['layer'] = doc_result[1]
                
        return result
        
    def batch_check_status(self, symbol_names: List[str]) -> Dict:
        """複数シンボルの処理状態を一括確認"""
        # DuckDBのIN句を使って効率的に取得
        placeholders = ','.join(['?' for _ in symbol_names])
        query = f"""
            SELECT symbol_name, summary, layer
            FROM documents
            WHERE symbol_name IN ({placeholders})
        """
        
        processed_docs = {}
        if symbol_names:
            results = self.doc_db.execute(query, symbol_names).fetchall()
            for name, summary, layer in results:
                processed_docs[name] = {
                    'summary': summary,
                    'layer': layer
                }
        
        # 結果を構築
        status_results = {}
        for symbol in symbol_names:
            if symbol in processed_docs:
                status_results[symbol] = {
                    'is_processed': True,
                    'summary': processed_docs[symbol]['summary'],
                    'layer': processed_docs[symbol]['layer'],
                    'exists': symbol in self.symbol_info
                }
            else:
                status_results[symbol] = {
                    'is_processed': False,
                    'exists': symbol in self.symbol_info
                }
                
        return status_results
        
    def get_symbol_with_deps_status(self, symbol_name: str) -> Dict:
        """シンボルの情報と依存関係の処理状態を一括取得"""
        if symbol_name not in self.symbol_info:
            return {'error': f'Symbol {symbol_name} not found'}
            
        info = self.symbol_info[symbol_name]
        
        # 依存シンボルのリストを取得
        deps_result = self.meta_db.execute("""
            SELECT to_symbol FROM dependencies
            WHERE from_symbol = ?
        """, (symbol_name,)).fetchall()
        
        depends_on = [row[0] for row in deps_result]
        
        # 依存シンボルの処理状態を一括確認
        deps_status = {}
        if depends_on:
            processed_deps = self.batch_check_status(depends_on)
            for dep, status in processed_deps.items():
                deps_status[dep] = {
                    'is_processed': status['is_processed'],
                    'summary': status.get('summary')
                }
        
        # ソースコード取得
        file_path = self.codebase_root / info['file']
        source = ''
        if file_path.exists():
            with open(file_path) as f:
                lines = f.readlines()
            start_line = info.get('start_line', 0)
            end_line = info.get('end_line', len(lines))
            source = ''.join(lines[start_line:end_line])
            
        return {
            'symbol': symbol_name,
            'type': info.get('type', 'unknown'),
            'file': str(info['file']),
            'source': source,
            'depends_on': depends_on,
            'deps_status': deps_status
        }
        
    def get_processed_summary(self, symbol_name: str) -> Dict:
        """処理済みシンボルの要約を返す"""
        if symbol_name not in self.processed_symbols:
            return {
                'symbol': symbol_name,
                'is_processed': False,
                'summary': None
            }
            
        result = self.doc_db.execute("""
            SELECT summary, content FROM documents 
            WHERE symbol_name = ?
        """, (symbol_name,)).fetchone()
        
        if result:
            return {
                'symbol': symbol_name,
                'is_processed': True,
                'summary': result[0],
                'has_full_content': bool(result[1])
            }
        return {
            'symbol': symbol_name,
            'is_processed': True,
            'summary': None
        }
        
    def get_symbol_source(self, symbol_name: str) -> Dict:
        """シンボルのソースコードを返す"""
        if symbol_name not in self.symbol_info:
            return {'error': f'Symbol {symbol_name} not found'}
            
        info = self.symbol_info[symbol_name]
        file_path = self.codebase_root / info['file']
        
        if not file_path.exists():
            return {'error': f'File {file_path} not found'}
            
        with open(file_path) as f:
            lines = f.readlines()
            
        start_line = info.get('start_line', 0)
        end_line = info.get('end_line', len(lines))
        source = ''.join(lines[start_line:end_line])
        
        return {
            'symbol': symbol_name,
            'type': info.get('type', 'unknown'),
            'file': str(info['file']),
            'source': source
        }
        
    def get_context_info(self, file_path: str) -> Dict:
        """ファイルのコンテキスト情報を返す"""
        full_path = self.codebase_root / file_path
        context = {}
        
        # READMEファイルを探す
        readme_path = full_path.parent / 'README'
        if readme_path.exists():
            with open(readme_path) as f:
                context['readme'] = f.read()
                
        # ファイルの先頭コメントを抽出
        if full_path.exists():
            with open(full_path) as f:
                lines = f.readlines()
                
            comment_lines = []
            in_comment = False
            
            for line in lines[:100]:
                if '/*' in line:
                    in_comment = True
                if in_comment:
                    comment_lines.append(line)
                if '*/' in line:
                    break
                    
            if comment_lines:
                context['file_comment'] = ''.join(comment_lines)
                
        return context

# MCPサーバーとして実行
if __name__ == "__main__":
    import sys
    server = PostgreSQLMCPServer(
        sys.argv[1] if len(sys.argv) > 1 else '/path/to/postgresql/src'
    )
    
    # MCPプロトコルに従って通信を処理
    while True:
        try:
            request = json.loads(input())
            method = request.get('method')
            params = request.get('params', {})
            
            result = None
            if method == 'check_symbol_status':
                result = server.check_symbol_status(params['symbol_name'])
            elif method == 'batch_check_status':
                result = server.batch_check_status(params['symbol_names'])
            elif method == 'get_symbol_with_deps_status':
                result = server.get_symbol_with_deps_status(params['symbol_name'])
            elif method == 'get_symbol_source':
                result = server.get_symbol_source(params['symbol_name'])
            elif method == 'get_processed_summary':
                result = server.get_processed_summary(params['symbol_name'])
            elif method == 'get_context_info':
                result = server.get_context_info(params['file_path'])
            else:
                result = {'error': f'Unknown method: {method}'}
                
            print(json.dumps(result))
            
        except EOFError:
            break
        except Exception as e:
            print(json.dumps({'error': str(e)}))
```

### 4. ドキュメント検索・閲覧ツール (scripts/doc_viewer.py)

```python
#!/usr/bin/env python3
"""
生成されたドキュメントを検索・閲覧するツール
"""
import duckdb
import argparse
from typing import Optional

class DocumentViewer:
    def __init__(self):
        self.doc_db = duckdb.connect('data/documents.duckdb', read_only=True)
        
    def search_documents(self, query: str, limit: int = 10):
        """ドキュメントを検索"""
        results = self.doc_db.execute("""
            SELECT 
                symbol_name,
                symbol_type,
                summary,
                layer
            FROM documents
            WHERE 
                symbol_name LIKE ? OR
                summary LIKE ? OR
                content LIKE ?
            LIMIT ?
        """, (f'%{query}%', f'%{query}%', f'%{query}%', limit)).fetchall()
        
        print(f"\n検索結果: '{query}'")
        print("=" * 60)
        for name, type_, summary, layer in results:
            print(f"\n[{type_}] {name} (Layer {layer})")
            print(f"  {summary[:100]}...")
            
    def get_document(self, symbol_name: str):
        """特定のドキュメントを取得"""
        result = self.doc_db.execute("""
            SELECT content, dependencies, related_symbols
            FROM documents
            WHERE symbol_name = ?
        """, (symbol_name,)).fetchone()
        
        if result:
            content, deps, related = result
            print(content)
            print("\n" + "=" * 60)
            print(f"Dependencies: {deps}")
            print(f"Related: {related}")
        else:
            print(f"Document not found: {symbol_name}")
            
    def show_statistics(self):
        """統計情報を表示"""
        stats = self.doc_db.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT symbol_type) as types,
                COUNT(DISTINCT layer) as layers,
                AVG(LENGTH(content)) as avg_length,
                MIN(created_at) as first_created,
                MAX(updated_at) as last_updated
            FROM documents
        """).fetchone()
        
        print("\nDocument Statistics")
        print("=" * 60)
        print(f"Total documents: {stats[0]}")
        print(f"Symbol types: {stats[1]}")
        print(f"Layers: {stats[2]}")
        print(f"Average length: {stats[3]:.0f} characters")
        print(f"First created: {stats[4]}")
        print(f"Last updated: {stats[5]}")
        
        # タイプ別統計
        type_stats = self.doc_db.execute("""
            SELECT symbol_type, COUNT(*) as count
            FROM documents
            GROUP BY symbol_type
            ORDER BY count DESC
        """).fetchall()
        
        print("\nBy type:")
        for type_, count in type_stats:
            print(f"  {type_}: {count}")

def main():
    parser = argparse.ArgumentParser(description='View PostgreSQL documentation')
    parser.add_argument('command', choices=['search', 'get', 'stats'])
    parser.add_argument('query', nargs='?', help='Search query or symbol name')
    parser.add_argument('--limit', type=int, default=10, help='Search result limit')
    
    args = parser.parse_args()
    viewer = DocumentViewer()
    
    if args.command == 'search':
        viewer.search_documents(args.query, args.limit)
    elif args.command == 'get':
        viewer.get_document(args.query)
    elif args.command == 'stats':
        viewer.show_statistics()

if __name__ == "__main__":
    main()
```

## DuckDB版の主な改善点

1. **高速な分析クエリ**: DuckDBは列指向ストレージで分析クエリが高速
2. **JSON型サポート**: 依存関係などをJSON型で効率的に格納
3. **大規模データ対応**: 大量のドキュメントも効率的に管理
4. **統合管理**: メタデータとドキュメントを別々のDBファイルで管理
5. **検索性能**: LIKE検索やJOIN操作が高速
6. **エクスポート機能**: 必要に応じてファイルにエクスポート可能

## 使用方法

```bash
# 1. 事前準備
pip install duckdb
python scripts/prepare_clusters.py

# 2. ドキュメント生成
python scripts/orchestrator.py

# 3. ドキュメント検索・閲覧
python scripts/doc_viewer.py search "heap_insert"
python scripts/doc_viewer.py get heap_insert
python scripts/doc_viewer.py stats

# 4. 必要に応じてエクスポート
# orchestrator.py内でexport_documents()を呼び出し
```

DuckDBにより、大規模なドキュメント管理がより効率的になり、検索や分析も高速に実行できます。
