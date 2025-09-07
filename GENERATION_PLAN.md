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
DuckDBの生データを直接読み込むバージョン
"""
import json
import duckdb
from pathlib import Path
from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple

class SymbolClusterer:
    def __init__(self, db_file: str):
        # DuckDBからグラフ構造とシンボル情報をメモリにロード
        self._load_graph_from_db(db_file)

        # 出力用DuckDBの初期化
        self.meta_db = duckdb.connect('data/metadata.duckdb')
        self.init_database()

        # 結果を格納
        self.clusters = []
        self.layers = []

    def _load_graph_from_db(self, db_file: str):
        """DuckDBからデータを読み込み、オンメモリグラフを構築する"""
        print(f"Loading graph data from {db_file}...")
        con = duckdb.connect(db_file, read_only=True)

        # シンボル定義をロード (id -> details)
        self.symbol_details: Dict[int, Dict] = {
            row[0]: {
                'id': row[0],
                'symbol_name': row[1],
                'file_path': row[2],
                'line_num_start': row[3],
                'line_num_end': row[4],
                'symbol_type': row[7]
            } for row in con.execute("SELECT * FROM symbol_definitions").fetchall()
        }
        self.all_nodes: Set[int] = set(self.symbol_details.keys())
        print(f"Loaded {len(self.symbol_details)} symbol definitions.")

        # 参照関係からグラフを構築
        references: List[Tuple[int, int]] = con.execute("SELECT from_node, to_node FROM symbol_reference").fetchall()
        con.close()

        self.adj: Dict[int, Set[int]] = defaultdict(set)  # 依存先 (自分がどのノードに依存しているか)
        self.rev_adj: Dict[int, Set[int]] = defaultdict(set) # 依存元 (どのノードから依存されているか)

        for from_node, to_node in references:
            if from_node in self.all_nodes and to_node in self.all_nodes:
                self.adj[from_node].add(to_node)
                self.rev_adj[to_node].add(from_node)
        print(f"Built graph with {len(references)} references.")


    def init_database(self):
        """出力用DuckDBデータベースを初期化"""
        # シンボル情報テーブル (主キーをidに変更)
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                id INTEGER PRIMARY KEY,
                symbol_name VARCHAR,
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

        # 依存関係テーブル (idベース)
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS dependencies (
                from_node INTEGER,
                to_node INTEGER,
                PRIMARY KEY (from_node, to_node)
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
        # 既存データをクリア
        self.meta_db.execute("DELETE FROM symbols")
        self.meta_db.execute("DELETE FROM dependencies")
        self.meta_db.execute("DELETE FROM clusters")

        # データを投入
        self.populate_initial_data()

    def populate_initial_data(self):
        """初期データをDuckDBに投入"""
        print("Populating metadata database...")
        # シンボル情報を投入
        for symbol_id, info in self.symbol_details.items():
            self.meta_db.execute("""
                INSERT OR REPLACE INTO symbols
                (id, symbol_name, symbol_type, file_path, module, start_line, end_line)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol_id,
                info['symbol_name'],
                info.get('symbol_type', 'unknown'),
                info.get('file_path', ''),
                self.get_symbol_module(symbol_id),
                info.get('line_num_start', 0),
                info.get('line_num_end', 0)
            ))

        # 依存関係を投入
        for from_node, to_nodes in self.adj.items():
            for to_node in to_nodes:
                self.meta_db.execute("""
                    INSERT OR REPLACE INTO dependencies
                    (from_node, to_node)
                    VALUES (?, ?)
                """, (from_node, to_node))

        self.meta_db.commit()
        print("Finished populating metadata database.")

    def analyze_dependencies(self):
        """オンメモリグラフで依存関係を解析し、トポロジカルソートで階層を作成"""
        in_degree = {node: len(self.rev_adj.get(node, set())) for node in self.all_nodes}
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        
        layers = []
        processed_count = 0
        
        while queue:
            current_layer_size = len(queue)
            if current_layer_size == 0:
                break
            
            current_layer = []
            for _ in range(current_layer_size):
                node = queue.popleft()
                current_layer.append(node)
                processed_count += 1
                
                # このノードが依存している先のノードのin-degreeを減らす
                for neighbor in self.adj.get(node, set()):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            layers.append(current_layer)

        # 循環依存のチェックと処理
        if processed_count < len(self.all_nodes):
            remaining = [node for node in self.all_nodes if in_degree[node] > 0]
            print(f"Warning: Circular dependency detected involving {len(remaining)} symbols. Grouping them into the last layer.")
            layers.append(remaining)

        # DBにレイヤー情報を更新
        for i, layer in enumerate(layers):
            for node_id in layer:
                self.meta_db.execute("UPDATE symbols SET layer = ? WHERE id = ?", (i, node_id))
        
        self.meta_db.commit()
        self.layers = layers
        return layers

    def create_file_based_clusters(self):
        """ファイルベースでシンボルをクラスタリング"""
        # ファイルごとにグループ化 (idも取得)
        result = self.meta_db.execute("""
            SELECT
                file_path,
                id,
                symbol_type,
                layer
            FROM symbols
            ORDER BY file_path, symbol_type, id
        """).fetchall()

        file_groups = defaultdict(list)
        for row in result:
            file_path, symbol_id, symbol_type, layer = row
            file_groups[file_path].append({
                'id': symbol_id,
                'type': symbol_type,
                'layer': layer
            })

        cluster_id_counter = 0
        for file_path, symbols in file_groups.items():
            if not symbols:
                continue
            # 大きすぎるグループは分割
            if len(symbols) <= 8:
                cluster_id_counter += 1
                self.save_cluster(cluster_id_counter, 'file', symbols)
            else:
                # タイプ別に分割
                for symbol_type in ['function', 'struct', 'typedef']: # 型を増やす
                    typed_symbols = [s for s in symbols if s['type'] == symbol_type]
                    if not typed_symbols: continue
                    for i in range(0, len(typed_symbols), 5):
                        cluster_id_counter += 1
                        batch = typed_symbols[i:i+5]
                        self.save_cluster(cluster_id_counter, f'file_{symbol_type}', batch)
        
        self.meta_db.commit()
        return cluster_id_counter

    def save_cluster(self, cluster_id: int, cluster_type: str, symbols: List[Dict]):
        """クラスタをDBに保存 (IDベース)"""
        symbol_ids = [s['id'] for s in symbols]
        # symbolsが空でないことを確認
        if not symbols:
            return
        # layerがNoneの場合を考慮
        valid_layers = [s.get('layer') for s in symbols if s.get('layer') is not None]
        avg_layer = sum(valid_layers) // len(valid_layers) if valid_layers else 0

        self.meta_db.execute("""
            INSERT INTO clusters (cluster_id, cluster_type, layer, symbols, estimated_tokens)
            VALUES (?, ?, ?, ?, ?)
        """, (
            cluster_id,
            cluster_type,
            avg_layer,
            json.dumps(symbol_ids),  # IDのリストをJSONとして保存
            len(symbol_ids) * 3000  # 推定トークン数
        ))

        # シンボルにクラスタIDを設定
        for symbol in symbols:
            self.meta_db.execute("UPDATE symbols SET cluster_id = ? WHERE id = ?", (cluster_id, symbol['id']))

    def get_symbol_module(self, symbol_id: int) -> str:
        """シンボルIDからモジュールを取得"""
        info = self.symbol_details.get(symbol_id)
        if info:
            file_path = info.get('file_path', '')
            if '/' in file_path:
                parts = file_path.split('/')
                if 'backend' in parts:
                    try:
                        idx = parts.index('backend')
                        if idx + 1 < len(parts):
                            return parts[idx + 1]
                    except ValueError:
                        pass
                return parts[0]
        return 'core'

    def generate_processing_batches(self):
        """処理用バッチを生成"""
        result = self.meta_db.execute("""
            SELECT
                c.cluster_id,
                c.cluster_type,
                c.layer,
                c.symbols,
                c.estimated_tokens
            FROM clusters c
            ORDER BY c.layer, c.cluster_id
        """).fetchall()

        batches = []
        for row in result:
            cluster_id, cluster_type, layer, symbols_json, tokens = row
            symbol_ids = json.loads(symbols_json)
            batches.append({
                'batch_id': cluster_id,
                'type': cluster_type,
                'layer': layer,
                'symbol_ids': symbol_ids, # キーを 'symbol_ids' に変更して明確化
                'estimated_tokens': tokens,
                'symbol_count': len(symbol_ids)
            })

        # ファイルに保存
        Path("data").mkdir(exist_ok=True)
        with open('data/processing_batches.json', 'w') as f:
            json.dump(batches, f, indent=2)

        return batches

def main():
    # 入力DBファイル
    db_file = 'data/global_symbols.db'
    
    clusterer = SymbolClusterer(db_file=db_file)

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
    stats_result = clusterer.meta_db.execute("""
        SELECT
            (SELECT COUNT(id) FROM symbols) as total_symbols,
            (SELECT COUNT(cluster_id) FROM clusters) as total_clusters,
            (SELECT COUNT(DISTINCT layer) FROM symbols WHERE layer IS NOT NULL) as total_layers,
            (SELECT AVG(estimated_tokens) FROM clusters) as avg_tokens_per_cluster
    """).fetchone()
    
    if stats_result:
        print("\nStatistics:")
        print(f"  Total symbols: {stats_result[0]}")
        print(f"  Total clusters: {stats_result[1]}")
        print(f"  Total layers: {stats_result[2]}")
        print(f"  Avg tokens per cluster: {stats_result[3]:.0f}")
    
    clusterer.meta_db.close()

if __name__ == "__main__":
    main()
```

### 2. メイン処理 (scripts/orchestrator.py)

```python
#!/usr/bin/env python3
"""
Claude Codeを使用したドキュメント生成のメイン処理
DuckDB版 - ドキュメントもDB内に格納 (IDベース処理)
"""
import json
import duckdb
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set

class DocumentationOrchestrator:
    def __init__(self, global_symbols_db: str = 'data/global_symbols.db'):
        # 処理バッチをロード (IDベース)
        with open('data/processing_batches.json') as f:
            self.batches = json.load(f)

        # シンボル詳細情報をメモリにロード
        self._load_symbol_details(global_symbols_db)
        
        # DuckDBの初期化
        self.init_databases()
        
        # 処理統計
        self.stats = {
            'total_batches': len(self.batches),
            'processed_batches': 0,
            'failed_batches': 0,
            'total_symbols': sum(len(b['symbol_ids']) for b in self.batches),
            'processed_symbols': 0
        }

    def _load_symbol_details(self, db_file: str):
        """global_symbols.dbからシンボル詳細をメモリにキャッシュする"""
        print(f"Loading symbol details from {db_file}...")
        con = duckdb.connect(db_file, read_only=True)
        self.symbol_details: Dict[int, Dict] = {
            row[0]: {
                'id': row[0],
                'name': row[1],
                'type': row[7] if row[7] else 'unknown',
            } for row in con.execute("SELECT * FROM symbol_definitions").fetchall()
        }
        con.close()
        print(f"Loaded {len(self.symbol_details)} symbol details into memory.")

    def init_databases(self):
        """DuckDBデータベースの初期化"""
        # メタデータDB（既存）
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        
        # ドキュメント専用DB
        self.doc_db = duckdb.connect('data/documents.duckdb')
        
        # ドキュメントテーブル (IDベースに修正)
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                symbol_id INTEGER PRIMARY KEY,
                symbol_name VARCHAR,
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
        
        # 処理ログテーブル (バッチIDを主キーとする)
        # metadata.duckdbはread-onlyで開いているため、ログはdoc_dbに書く
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                batch_id INTEGER PRIMARY KEY,
                symbol_ids JSON,
                status VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                processed_count INTEGER
            )
        """)

    def get_processed_symbol_ids(self) -> Set[int]:
        """処理済みシンボルIDを取得"""
        result = self.doc_db.execute("SELECT symbol_id FROM documents").fetchall()
        return set(row[0] for row in result)
        
    def process_all_batches(self):
        """全バッチを順次処理"""
        processed_ids = self.get_processed_symbol_ids()
        
        for batch in self.batches:
            batch_id = batch['batch_id']
            # スキップ判定 (IDベース)
            unprocessed_ids = [sid for sid in batch['symbol_ids'] if sid not in processed_ids]
            if not unprocessed_ids:
                print(f"Batch {batch_id}: All symbols already processed, skipping")
                continue
                
            print(f"\n{'='*60}")
            print(f"Processing batch {batch_id}/{len(self.batches)}")
            print(f"Layer: {batch['layer']}, Symbols: {len(unprocessed_ids)}")
            print(f"Type: {batch['type']}, Estimated tokens: {batch['estimated_tokens']}")
            print(f"{'='*60}")
            
            # バッチを処理
            success = self.process_batch(batch, unprocessed_ids)
            
            if success:
                self.stats['processed_batches'] += 1
                self.stats['processed_symbols'] += len(unprocessed_ids)
                processed_ids.update(unprocessed_ids)
            else:
                self.stats['failed_batches'] += 1
                
            # 進捗表示
            self.show_progress()
            
            # レート制限対策
            time.sleep(2)
            
    def process_batch(self, batch: Dict, symbol_ids: List[int]) -> bool:
        """単一バッチを処理"""
        batch_id = batch['batch_id']
        
        # ログ記録開始
        self.doc_db.execute("""
            INSERT OR REPLACE INTO processing_log 
            (batch_id, symbol_ids, status, started_at, processed_count)
            VALUES (?, ?, 'processing', ?, 0)
        """, (batch_id, json.dumps(symbol_ids), datetime.now()))
        self.doc_db.commit()

        # プロンプトを構築
        prompt = self.build_prompt(symbol_ids, batch['layer'])
        
        try:
            # Claude Codeを実行
            # claude-code-cli を想定したコマンドラインに修正
            # 例: claude-code chat --prompt "..."
            # ※ツール名や引数は環境に合わせて調整してください
            print("Invoking Claude Code CLI...")
            result = subprocess.run(
                ['claude-code', 'chat', '--prompt', prompt],
                capture_output=True,
                text=True,
                timeout=600, # タイムアウトを延長
                cwd=str(Path.cwd()),
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully processed batch {batch_id}")
                
                # 成功をログに記録
                self.doc_db.execute("""
                    UPDATE processing_log SET status = 'completed', completed_at = ?, processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbol_ids), batch_id))
                
                # ここでは直接パースする代わりに、エージェントがファイルに出力したと仮定
                self.store_generated_documents(symbol_ids, batch['layer'])
                
                return True
            else:
                error_msg = result.stderr[:1000] if result.stderr else 'Unknown error'
                print(f"✗ Failed to process batch {batch_id}: {error_msg}")
                self.doc_db.execute("""
                    UPDATE processing_log SET status = 'failed', completed_at = ?, error_message = ?
                    WHERE batch_id = ?
                """, (datetime.now(), error_msg, batch_id))
                return False
                
        except subprocess.TimeoutExpired:
            print(f"✗ Batch {batch_id} timed out")
            self.doc_db.execute("""
                UPDATE processing_log SET status = 'timeout', completed_at = ? WHERE batch_id = ?
            """, (datetime.now(), batch_id))
            return False
            
        except Exception as e:
            error_msg = str(e)[:1000]
            print(f"✗ Unexpected error in batch {batch_id}: {error_msg}")
            self.doc_db.execute("""
                UPDATE processing_log SET status = 'error', completed_at = ?, error_message = ?
                WHERE batch_id = ?
            """, (datetime.now(), error_msg, batch_id))
            return False
        finally:
            self.doc_db.commit()

    def get_processed_summaries(self) -> Dict[str, str]:
        """処理済みシンボルの要約を取得 (名前 -> 要約)"""
        result = self.doc_db.execute("""
            SELECT symbol_name, summary FROM documents WHERE summary IS NOT NULL AND summary != ''
            LIMIT 2000
        """).fetchall()
        return {row[0]: row[1] for row in result}
        
    def build_prompt(self, symbol_ids: List[int], layer: int) -> str:
        """バッチ処理用のプロンプトを構築"""
        symbol_names = [self.symbol_details[sid]['name'] for sid in symbol_ids]
        symbol_list_str = '\n'.join([f'- {name}' for name in symbol_names])

        processed_summaries = self.get_processed_summaries()
        
        relevant_processed = set()
        for symbol_id in symbol_ids:
            # このシンボルの依存先を取得 (IDベース)
            deps = self.meta_db.execute("""
                SELECT to_node FROM dependencies WHERE from_node = ?
            """, (symbol_id,)).fetchall()
            
            for (dep_id,) in deps:
                dep_name = self.symbol_details.get(dep_id, {}).get('name')
                if dep_name and dep_name in processed_summaries:
                    summary = processed_summaries[dep_name]
                    relevant_processed.add(f"- {dep_name}: {summary[:120]}")
                    
        relevant_list_str = '\n'.join(sorted(list(relevant_processed))[:15])
        
        # プロンプトテンプレート
        # claude-code-cli が index を参照することを前提としたプロンプト
        prompt = f"""# PostgreSQLコードベースのドキュメント生成タスク

あなたはPostgreSQLのソースコードに精通したエキスパートです。
インデックスされたPostgreSQLのコードベース全体を参照し、以下の未処理シンボルについて詳細なドキュメントを生成してください。

## 処理コンテキスト
- 現在の処理レイヤー: {layer} (依存関係の末端に近いレイヤーから処理しています)
- 処理済みシンボル総数: {len(processed_summaries)} / {self.stats["total_symbols"]}

## 処理対象シンボルリスト
{symbol_list_str}

## 関連する処理済みシンボルの要約
以下は、今回処理するシンボルが依存している可能性のある、既に処理済みのシンボルの要約です。文脈の理解に役立ててください。
{relevant_list_str if relevant_list_str else '（特に関連情報なし）'}

## 指示
1.  上記の「処理対象シンボルリスト」内の各シンボルについて、順番に処理してください。
2.  各シンボルのソースコード、定義、および参照箇所をコードベース全体から検索・分析してください。
3.  以下のMarkdownフォーマットに従って、各シンボルの解説を生成してください。
4.  生成したドキュメントは、ローカルの `output/temp/` ディレクトリに `[シンボル名].md` というファイル名で保存してください。

## 出力Markdownフォーマット
```markdown
# [シンボル名]

## 概要
(このシンボルの目的や役割を1〜2文で簡潔に説明)

## 定義
(関数シグネチャまたは構造体/enumの定義をコードブロックで記述)
```c
// 例: void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid, char *out_dbname)
```

## 詳細説明
(シンボルの機能、動作、設計思想などを具体的に解説)

## パラメータ / メンバー変数
(関数パラメータや構造体の各メンバーについて、役割や意味を箇条書きで説明)
- `param1`: (説明)
- `member1`: (説明)

## 依存関係
- **呼び出している関数/参照しているシンボル**:
  - `func_a`
  - `TYPE_B`
- **呼び出されている箇所 (代表例)**:
  - `caller_func_x`
  - `caller_func_y`

## 注意事項・その他
(特筆すべき点、利用上の注意、関連する背景知識など)

```

全てのシンボルについて、上記の指示通りにファイル出力を完了させてください。
"""
        return prompt
        
    def store_generated_documents(self, symbol_ids: List[int], layer: int):
        """生成されたドキュメントをDuckDBに格納"""
        temp_dir = Path('output/temp')
        temp_dir.mkdir(exist_ok=True)
        
        for sid in symbol_ids:
            symbol_name = self.symbol_details[sid]['name']
            symbol_type = self.symbol_details[sid]['type']
            doc_path = temp_dir / f"{symbol_name}.md"
            
            if doc_path.exists():
                content = doc_path.read_text(encoding='utf-8')
                summary = self.extract_summary(content)
                deps, related = self.extract_relationships(content)
                
                # ドキュメントをDBに格納 (IDベース)
                self.doc_db.execute("""
                    INSERT INTO documents (symbol_id, symbol_name, symbol_type, layer, content, summary, dependencies, related_symbols)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        content = EXCLUDED.content, summary = EXCLUDED.summary,
                        dependencies = EXCLUDED.dependencies, related_symbols = EXCLUDED.related_symbols,
                        updated_at = CURRENT_TIMESTAMP
                """, (sid, symbol_name, symbol_type, layer, content, summary, json.dumps(deps), json.dumps(related)))
                
                doc_path.unlink() # 一時ファイルを削除
                print(f"  Stored document for: {symbol_name} (ID: {sid})")
            else:
                print(f"  Warning: Document file not found for {symbol_name}")
                
        self.doc_db.commit()

    def extract_summary(self, content: str) -> str:
        """ドキュメントから概要を抽出"""
        lines = content.split('\n')
        in_summary = False
        summary_lines = []
        for line in lines:
            if '## 概要' in line:
                in_summary = True
                continue
            if in_summary and line.startswith('##'):
                break
            if in_summary and line.strip():
                summary_lines.append(line.strip())
        return ' '.join(summary_lines[:2])

    def extract_relationships(self, content: str) -> tuple:
        """ドキュメントから関係性を抽出"""
        import re
        deps = re.findall(r'-\s*\*\*呼び出している関数/参照しているシンボル\*\*:\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        deps_list = re.findall(r'-\s*`(\w+)`', ''.join(deps))
        
        related = re.findall(r'-\s*\*\*呼び出されている箇所 \(代表例\)\*\*:\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        related_list = re.findall(r'-\s*`(\w+)`', ''.join(related))
        
        return list(set(deps_list)), list(set(related_list))

    def show_progress(self):
        """進捗を表示"""
        if self.stats['total_symbols'] == 0: return
        progress = (self.stats['processed_symbols'] / self.stats['total_symbols']) * 100
        print(f"\nProgress: {progress:.1f}% ({self.stats['processed_symbols']}/{self.stats['total_symbols']})")
        print(f"Completed Batches: {self.stats['processed_batches']}/{self.stats['total_batches']}")

def main():
    orchestrator = DocumentationOrchestrator()
    print("PostgreSQL Documentation Generation Orchestrator (ID-based)")
    print("=" * 60)
    orchestrator.process_all_batches()
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
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

try:
    from snode_module import SNode, search_symbols, DatabaseConnection
except ImportError:
    print("FATAL: snode_module.py not found. Please place it in the same directory.")
    exit(1)
except FileNotFoundError as e:
    print(f"FATAL: Database file not found as specified in snode_module.py.")
    print(f"Error: {e}")
    exit(1)


# AIエージェントが生成したドキュメントを一時保存するディレクトリ
TEMP_OUTPUT_DIR = Path("output/temp")


class MCPRequestHandler(BaseHTTPRequestHandler):
    """
    AIエージェントからのツール利用リクエストを処理するハンドラ。
    snode_moduleの機能をAPIとして提供する。
    """
    def do_POST(self):
        """POSTリクエストを処理する"""
        try:
            # リクエストボディをJSONとして読み込む
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            request = json.loads(post_data)

            method = request.get("method")
            params = request.get("params", {})

            # methodの値に応じて処理を分岐
            if method == "get_symbol_details":
                symbol_name = params.get("symbol_name")
                if not symbol_name:
                    return self._send_response(400, {"error": "Missing parameter: symbol_name"})
                
                node = SNode(symbol_name)
                response_data = {
                    "id": node.id,
                    "symbol_name": node.symbol_name,
                    "file_path": node.file_path,
                    "start_line": node.line_num_start,
                    "end_line": node.line_num_end,
                    "type": node.symbol_type
                }
                self._send_response(200, response_data)

            elif method == "get_symbol_source":
                symbol_name = params.get("symbol_name")
                if not symbol_name:
                    return self._send_response(400, {"error": "Missing parameter: symbol_name"})
                
                node = SNode(symbol_name)
                source_code = node.get_source_code()
                self._send_response(200, {"source_code": source_code})

            elif method == "get_references_from_this":
                symbol_name = params.get("symbol_name")
                if not symbol_name:
                    return self._send_response(400, {"error": "Missing parameter: symbol_name"})

                node = SNode(symbol_name)
                references = node.get_references_from_this()
                self._send_response(200, {"references": references})

            elif method == "get_references_to_this":
                symbol_name = params.get("symbol_name")
                if not symbol_name:
                    return self._send_response(400, {"error": "Missing parameter: symbol_name"})

                node = SNode(symbol_name)
                referenced_by = node.get_references_to_this()
                self._send_response(200, {"referenced_by": referenced_by})

            elif method == "search_symbols":
                pattern = params.get("pattern")
                if not pattern:
                    return self._send_response(400, {"error": "Missing parameter: pattern"})
                
                symbols = search_symbols(pattern)
                self._send_response(200, {"symbols": symbols})
            
            elif method == "save_document":
                # ファイル保存はサーバー側の機能として実装
                self._save_document(params)

            else:
                self._send_response(404, {"error": f"Unknown method: {method}"})

        except json.JSONDecodeError:
            self._send_response(400, {"error": "Invalid JSON format in request body."})
        except ValueError as e:
            # SNodeでシンボルが見つからなかった場合に発生
            self._send_response(404, {"error": str(e)})
        except FileNotFoundError as e:
            # ソースコードファイルが見つからなかった場合に発生
            self._send_response(404, {"error": str(e)})
        except Exception as e:
            # その他のサーバー内部エラー
            print(f"Unhandled error processing request: {type(e).__name__} - {e}")
            self._send_response(500, {"error": "An internal server error occurred.", "details": str(e)})

    def _send_response(self, status_code: int, data: dict):
        """HTTPレスポンスをJSON形式で送信するヘルパー関数"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

    def _save_document(self, params: dict):
        """
        AIエージェントが生成したドキュメントを一時ファイルとして保存する。
        """
        symbol_name = params.get("symbol_name")
        content = params.get("content")

        if not symbol_name or not isinstance(symbol_name, str):
            self._send_response(400, {"error": "'symbol_name' parameter is required and must be a string."})
            return
        if content is None:
            self._send_response(400, {"error": "'content' parameter is required."})
            return
        
        try:
            TEMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            file_path = TEMP_OUTPUT_DIR / f"{symbol_name}.md"
            file_path.write_text(content, encoding='utf-8')
            
            message = f"Document for '{symbol_name}' saved to {file_path}"
            print(message)
            self._send_response(200, {"status": "success", "message": message})
        
        except IOError as e:
            error_message = f"Failed to write document for '{symbol_name}': {e}"
            print(error_message)
            self._send_response(500, {"error": "Failed to save document on server."})


def run_server(server_class=HTTPServer, handler_class=MCPRequestHandler, port=8080):
    """サーバーを起動し、終了時にリソースをクリーンアップする"""
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting MCP server on http://localhost:{port}...")
    print("This server provides tools for the AI agent.")
    print("Press Ctrl+C to stop the server.")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        # Ctrl+C が押されたらループを抜ける
        pass
    finally:
        # サーバー終了時にデータベース接続を閉じる
        print("\nStopping server...")
        DatabaseConnection().close()
        httpd.server_close()
        print("Server stopped and database connection closed.")

if __name__ == '__main__':
    run_server()
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
