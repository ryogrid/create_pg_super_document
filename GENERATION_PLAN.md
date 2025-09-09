We present an improved version using DuckDB. DuckDB excels at analytical workloads and is better suited for large-scale document management.

```
postgresql-docs/
├── .claude/
│   ├── settings.json
│   └── settings.local.json
├── .mcp.json
├── CLAUDE.md
├── scripts/
│   ├── prepare_clusters.py      # Pre-clustering processing
│   ├── orchestrator.py          # Main processing
│   ├── mcp_server.py           # MCP server
│   └── cache_manager.py        # Cache management
├── data/
│   ├── symbol_clusters.json    # Clustering results
│   ├── dependency_layers.json  # Dependency hierarchy
│   ├── processed_cache.db      # Processed cache
│   ├── Other DB files
│   └── current_batch.json      # Current batch information
├── output/temp/               # Temporary file storage
```

## Complete DuckDB Version Implementation

### 1. Pre-processing: Clustering (scripts/prepare_clusters.py)

```python
#!/usr/bin/env python3
"""
Pre-cluster symbols to prepare efficient batches
DuckDB version that directly reads raw data
"""
import json
import duckdb
from pathlib import Path
from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple

class SymbolClusterer:
    def __init__(self, db_file: str):
        # Load graph structure and symbol information from DuckDB into memory
        self._load_graph_from_db(db_file)

        # Initialize output DuckDB
        self.meta_db = duckdb.connect('data/metadata.duckdb')
        self.init_database()

        # Store results
        self.clusters = []
        self.layers = []

    def _load_graph_from_db(self, db_file: str):
        """Load data from DuckDB and build in-memory graph"""
        print(f"Loading graph data from {db_file}...")
        con = duckdb.connect(db_file, read_only=True)

        # Load symbol definitions (id -> details)
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

        # Build graph from reference relationships
        references: List[Tuple[int, int]] = con.execute("SELECT from_node, to_node FROM symbol_reference").fetchall()
        con.close()

        self.adj: Dict[int, Set[int]] = defaultdict(set)  # Dependencies (which nodes this node depends on)
        self.rev_adj: Dict[int, Set[int]] = defaultdict(set) # Dependents (which nodes depend on this node)

        for from_node, to_node in references:
            if from_node in self.all_nodes and to_node in self.all_nodes:
                self.adj[from_node].add(to_node)
                self.rev_adj[to_node].add(from_node)
        print(f"Built graph with {len(references)} references.")


    def init_database(self):
        """Initialize output DuckDB database"""
        # Symbol information table (change primary key to id)
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

        # Dependency table (id-based)
        self.meta_db.execute("""
            CREATE TABLE IF NOT EXISTS dependencies (
                from_node INTEGER,
                to_node INTEGER,
                PRIMARY KEY (from_node, to_node)
            )
        """)

        # Cluster table
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
        # Clear existing data
        self.meta_db.execute("DELETE FROM symbols")
        self.meta_db.execute("DELETE FROM dependencies")
        self.meta_db.execute("DELETE FROM clusters")

        # Populate data
        self.populate_initial_data()

    def populate_initial_data(self):
        """Populate initial data into DuckDB"""
        print("Populating metadata database...")
        # Insert symbol information
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

        # Insert dependencies
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
        """Analyze dependencies with in-memory graph and create layers using topological sort"""
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
                
                # Reduce in-degree for nodes that this node depends on
                for neighbor in self.adj.get(node, set()):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            layers.append(current_layer)

        # Check and handle circular dependencies
        if processed_count < len(self.all_nodes):
            remaining = [node for node in self.all_nodes if in_degree[node] > 0]
            print(f"Warning: Circular dependency detected involving {len(remaining)} symbols. Grouping them into the last layer.")
            layers.append(remaining)

        # Update layer information in DB
        for i, layer in enumerate(layers):
            for node_id in layer:
                self.meta_db.execute("UPDATE symbols SET layer = ? WHERE id = ?", (i, node_id))
        
        self.meta_db.commit()
        self.layers = layers
        return layers

    def create_file_based_clusters(self):
        """Cluster symbols based on files"""
        # Group by file (also get id)
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
            # Split groups that are too large
            if len(symbols) <= 8:
                cluster_id_counter += 1
                self.save_cluster(cluster_id_counter, 'file', symbols)
            else:
                # Split by type
                for symbol_type in ['function', 'struct', 'typedef']: # Add more types
                    typed_symbols = [s for s in symbols if s['type'] == symbol_type]
                    if not typed_symbols: continue
                    for i in range(0, len(typed_symbols), 5):
                        cluster_id_counter += 1
                        batch = typed_symbols[i:i+5]
                        self.save_cluster(cluster_id_counter, f'file_{symbol_type}', batch)
        
        self.meta_db.commit()
        return cluster_id_counter

    def save_cluster(self, cluster_id: int, cluster_type: str, symbols: List[Dict]):
        """Save cluster to DB (ID-based)"""
        symbol_ids = [s['id'] for s in symbols]
        # Verify symbols is not empty
        if not symbols:
            return
        # Handle cases where layer is None
        valid_layers = [s.get('layer') for s in symbols if s.get('layer') is not None]
        avg_layer = sum(valid_layers) // len(valid_layers) if valid_layers else 0

        self.meta_db.execute("""
            INSERT INTO clusters (cluster_id, cluster_type, layer, symbols, estimated_tokens)
            VALUES (?, ?, ?, ?, ?)
        """, (
            cluster_id,
            cluster_type,
            avg_layer,
            json.dumps(symbol_ids),  # Save ID list as JSON
            len(symbol_ids) * 3000  # Estimated token count
        ))

        # Set cluster ID for symbols
        for symbol in symbols:
            self.meta_db.execute("UPDATE symbols SET cluster_id = ? WHERE id = ?", (cluster_id, symbol['id']))

    def get_symbol_module(self, symbol_id: int) -> str:
        """Get module from symbol ID"""
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
        """Generate processing batches"""
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
                'symbol_ids': symbol_ids, # Change key to 'symbol_ids' for clarity
                'estimated_tokens': tokens,
                'symbol_count': len(symbol_ids)
            })

        # Save to file
        Path("data").mkdir(exist_ok=True)
        with open('data/processing_batches.json', 'w') as f:
            json.dump(batches, f, indent=2)

        return batches

def main():
    # Input DB file
    db_file = 'data/global_symbols.db'
    
    clusterer = SymbolClusterer(db_file=db_file)

    # Create dependency layers
    layers = clusterer.analyze_dependencies()
    print(f"Created {len(layers)} dependency layers")

    # Create clusters
    num_clusters = clusterer.create_file_based_clusters()
    print(f"Created {num_clusters} clusters")

    # Generate processing batches
    batches = clusterer.generate_processing_batches()
    print(f"Generated {len(batches)} processing batches")

    # Display statistics
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

### 2. Main Processing (scripts/orchestrator.py)

```python
#!/usr/bin/env python3
"""
Main processing for document generation using Claude Code
DuckDB version - Store documents in DB (ID-based processing)
"""
import json
import duckdb
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set

class DocumentationOrchestrator:
    def __init__(self, global_symbols_db: str = 'global_symbols.db'):
        # Load processing batches (ID-based)
        with open('data/processing_batches.json') as f:
            self.batches = json.load(f)

        # Load symbol details into memory
        self._load_symbol_details(global_symbols_db)
        
        # Initialize DuckDB
        self.init_databases()
        
        # Processing statistics
        self.stats = {
            'total_batches': len(self.batches),
            'processed_batches': 0,
            'failed_batches': 0,
            'total_symbols': sum(len(b['symbol_ids']) for b in self.batches),
            'processed_symbols': 0
        }

    def _load_symbol_details(self, db_file: str):
        """Cache symbol details from global_symbols.db into memory"""
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
        """Initialize DuckDB databases"""
        # Metadata DB (existing)
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        
        # Document-specific DB
        self.doc_db = duckdb.connect('data/documents.duckdb')
        
        # Document table (modified to ID-based)
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
        
        # Processing log table (batch ID as primary key)
        # Since metadata.duckdb is opened read-only, write logs to doc_db
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
        """Get processed symbol IDs"""
        result = self.doc_db.execute("SELECT symbol_id FROM documents").fetchall()
        return set(row[0] for row in result)
        
    def process_all_batches(self):
        """Process all batches sequentially"""
        processed_ids = self.get_processed_symbol_ids()
        
        for batch in self.batches:
            batch_id = batch['batch_id']
            # Skip determination (ID-based)
            unprocessed_ids = [sid for sid in batch['symbol_ids'] if sid not in processed_ids]
            if not unprocessed_ids:
                print(f"Batch {batch_id}: All symbols already processed, skipping")
                continue
                
            print(f"\n{'='*60}")
            print(f"Processing batch {batch_id}/{len(self.batches)}")
            print(f"Layer: {batch['layer']}, Symbols: {len(unprocessed_ids)}")
            print(f"Type: {batch['type']}, Estimated tokens: {batch['estimated_tokens']}")
            print(f"{'='*60}")
            
            # Process batch
            success = self.process_batch(batch, unprocessed_ids)
            
            if success:
                self.stats['processed_batches'] += 1
                self.stats['processed_symbols'] += len(unprocessed_ids)
                processed_ids.update(unprocessed_ids)
            else:
                self.stats['failed_batches'] += 1
                
            # Show progress
            self.show_progress()
            
            # Rate limiting
            time.sleep(2)
            
    def process_batch(self, batch: Dict, symbol_ids: List[int]) -> bool:
        """Process a single batch"""
        batch_id = batch['batch_id']
        
        # Start logging
        self.doc_db.execute("""
            INSERT OR REPLACE INTO processing_log 
            (batch_id, symbol_ids, status, started_at, processed_count)
            VALUES (?, ?, 'processing', ?, 0)
        """, (batch_id, json.dumps(symbol_ids), datetime.now()))
        self.doc_db.commit()

        # Build prompt
        prompt = self.build_prompt(symbol_ids, batch['layer'])
        
        try:
            # Execute Claude Code
            # Modified for claude-code-cli command line
            # Example: claude-code chat --prompt "..."
            # ※ Adjust tool name and arguments according to your environment
            print("Invoking Claude Code CLI...")
            result = subprocess.run(
                ['claude-code', 'chat', '--prompt', prompt],
                capture_output=True,
                text=True,
                timeout=600, # Extended timeout
                cwd=str(Path.cwd()),
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully processed batch {batch_id}")
                
                # Log success
                self.doc_db.execute("""
                    UPDATE processing_log SET status = 'completed', completed_at = ?, processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbol_ids), batch_id))
                
                # Instead of parsing directly, assume agent outputs to file
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
        """Get summaries of processed symbols (name -> summary)"""
        result = self.doc_db.execute("""
            SELECT symbol_name, summary FROM documents WHERE summary IS NOT NULL AND summary != ''
            LIMIT 2000
        """).fetchall()
        return {row[0]: row[1] for row in result}
        
    def build_prompt(self, symbol_ids: List[int], layer: int) -> str:
        """Build prompt for batch processing"""
        symbol_names = [self.symbol_details[sid]['name'] for sid in symbol_ids]
        symbol_list_str = '\n'.join([f'- {name}' for name in symbol_names])

        processed_summaries = self.get_processed_summaries()
        
        relevant_processed = set()
        for symbol_id in symbol_ids:
            # Get dependencies of this symbol (ID-based)
            deps = self.meta_db.execute("""
                SELECT to_node FROM dependencies WHERE from_node = ?
            """, (symbol_id,)).fetchall()
            
            for (dep_id,) in deps:
                dep_name = self.symbol_details.get(dep_id, {}).get('name')
                if dep_name and dep_name in processed_summaries:
                    summary = processed_summaries[dep_name]
                    relevant_processed.add(f"- {dep_name}: {summary[:120]}")
                    
        relevant_list_str = '\n'.join(sorted(list(relevant_processed))[:15])
        
        # Prompt template
        # Prompt assuming claude-code-cli references index
        prompt = f"""# PostgreSQL Codebase Documentation Generation Task

You are an expert familiar with PostgreSQL source code.
Please reference the entire indexed PostgreSQL codebase and generate detailed documentation for the following unprocessed symbols.

## Processing Context
- Current processing layer: {layer} (processing from layers closer to dependency endpoints)
- Total processed symbols: {len(processed_summaries)} / {self.stats["total_symbols"]}

## Target Symbol List
{symbol_list_str}

## Related Processed Symbol Summaries
The following are summaries of already processed symbols that the current symbols may depend on. Use them to understand the context.
{relevant_list_str if relevant_list_str else '(No particular related information)'}

## Instructions
1.  Process each symbol in the above "Target Symbol List" in order.
2.  Search and analyze the source code, definitions, and reference locations for each symbol throughout the codebase.
3.  Generate explanations for each symbol following the Markdown format below.
4.  Save the generated documents in the local `output/temp/` directory with the filename `[symbol_name].md`.

## Output Markdown Format
```markdown
# [Symbol Name]

## Overview
(Concisely explain the purpose and role of this symbol in 1-2 sentences)

## Definition
(Describe function signature or struct/enum definition in code block)
```c
// Example: void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid, char *out_dbname)
```

## Detailed Description
(Specifically explain the symbol's functionality, behavior, design philosophy, etc.)

## Parameters / Member Variables
(List and explain the role and meaning of function parameters or struct members)
- `param1`: (description)
- `member1`: (description)

## Dependencies
- **Called functions/Referenced symbols**:
  - `func_a`
  - `TYPE_B`
- **Called from (representative examples)**:
  - `caller_func_x`
  - `caller_func_y`

## Notes & Other Information
(Notable points, usage precautions, related background knowledge, etc.)

```

Please complete file output for all symbols according to the above instructions.
"""
        return prompt
        
    def store_generated_documents(self, symbol_ids: List[int], layer: int):
        """Store generated documents in DuckDB"""
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
                
                # Store document in DB (ID-based)
                self.doc_db.execute("""
                    INSERT INTO documents (symbol_id, symbol_name, symbol_type, layer, content, summary, dependencies, related_symbols)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        content = EXCLUDED.content, summary = EXCLUDED.summary,
                        dependencies = EXCLUDED.dependencies, related_symbols = EXCLUDED.related_symbols,
                        updated_at = CURRENT_TIMESTAMP
                """, (sid, symbol_name, symbol_type, layer, content, summary, json.dumps(deps), json.dumps(related)))
                
                doc_path.unlink() # Delete temporary file
                print(f"  Stored document for: {symbol_name} (ID: {sid})")
            else:
                print(f"  Warning: Document file not found for {symbol_name}")
                
        self.doc_db.commit()

    def extract_summary(self, content: str) -> str:
        """Extract overview from document"""
        lines = content.split('\n')
        in_summary = False
        summary_lines = []
        for line in lines:
            if '## Overview' in line:
                in_summary = True
                continue
            if in_summary and line.startswith('##'):
                break
            if in_summary and line.strip():
                summary_lines.append(line.strip())
        return ' '.join(summary_lines[:2])

    def extract_relationships(self, content: str) -> tuple:
        """Extract relationships from document"""
        import re
        deps = re.findall(r'-\s*\*\*Called functions/Referenced symbols\*\*:\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        deps_list = re.findall(r'-\s*`(\w+)`', ''.join(deps))
        
        related = re.findall(r'-\s*\*\*Called from \(representative examples\)\*\*:\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        related_list = re.findall(r'-\s*`(\w+)`', ''.join(related))
        
        return list(set(deps_list)), list(set(related_list))

    def show_progress(self):
        """Show progress"""
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

### 3. MCP Server (scripts/mcp_server.py)

```python
#!/usr/bin/env python3
"""
MCP server for PostgreSQL codebase
DuckDB version
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


# Directory for temporarily storing documents generated by AI agent
TEMP_OUTPUT_DIR = Path("output/temp")


class MCPRequestHandler(BaseHTTPRequestHandler):
    """
    Handler for processing tool usage requests from AI agents.
    Provides snode_module functionality as API.
    """
    def do_POST(self):
        """Process POST requests"""
        try:
            # Read request body as JSON
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            request = json.loads(post_data)

            method = request.get("method")
            params = request.get("params", {})

            # Branch processing based on method value
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
                # File saving implemented as server-side functionality
                self._save_document(params)

            else:
                self._send_response(404, {"error": f"Unknown method: {method}"})

        except json.JSONDecodeError:
            self._send_response(400, {"error": "Invalid JSON format in request body."})
        except ValueError as e:
            # Raised when symbol not found in SNode
            self._send_response(404, {"error": str(e)})
        except FileNotFoundError as e:
            # Raised when source code file not found
            self._send_response(404, {"error": str(e)})
        except Exception as e:
            # Other internal server errors
            print(f"Unhandled error processing request: {type(e).__name__} - {e}")
            self._send_response(500, {"error": "An internal server error occurred.", "details": str(e)})

    def _send_response(self, status_code: int, data: dict):
        """Helper function to send HTTP response in JSON format"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

    def _save_document(self, params: dict):
        """
        Save documents generated by AI agent as temporary files.
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
    """Start server and clean up resources on exit"""
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting MCP server on http://localhost:{port}...")
    print("This server provides tools for the AI agent.")
    print("Press Ctrl+C to stop the server.")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        # Exit loop when Ctrl+C is pressed
        pass
    finally:
        # Close database connection when server stops
        print("\nStopping server...")
        DatabaseConnection().close()
        httpd.server_close()
        print("Server stopped and database connection closed.")

if __name__ == '__main__':
    run_server()
```

### 4. Document Search & Viewing Tool (scripts/doc_viewer.py)

```python
#!/usr/bin/env python3
"""
Tool for searching and viewing generated documents
"""
import duckdb
import argparse
from typing import Optional

class DocumentViewer:
    def __init__(self):
        self.doc_db = duckdb.connect('data/documents.duckdb', read_only=True)
        
    def search_documents(self, query: str, limit: int = 10):
        """Search documents"""
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
        
        print(f"\nSearch results: '{query}'")
        print("=" * 60)
        for name, type_, summary, layer in results:
            print(f"\n[{type_}] {name} (Layer {layer})")
            print(f"  {summary[:100]}...")
            
    def get_document(self, symbol_name: str):
        """Get specific document"""
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
        """Show statistics"""
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
        
        # Statistics by type
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

## Key Improvements in DuckDB Version

1. **Fast analytical queries**: DuckDB's columnar storage enables fast analytical queries
2. **JSON type support**: Efficiently store dependencies and other data as JSON
3. **Large-scale data support**: Efficiently manage large volumes of documents
4. **Integrated management**: Manage metadata and documents in separate DB files
5. **Search performance**: Fast LIKE searches and JOIN operations
6. **Export functionality**: Export to files as needed

## Usage

```bash
# 1. Preparation
pip install duckdb
python scripts/prepare_clusters.py

# 2. Document generation
python scripts/orchestrator.py

# 3. Document search & viewing
python scripts/doc_viewer.py search "heap_insert"
python scripts/doc_viewer.py get heap_insert
python scripts/doc_viewer.py stats

# 4. Export as needed
# Call export_documents() in orchestrator.py
```

With DuckDB, large-scale document management becomes more efficient, and searches and analysis can be executed at high speed.
