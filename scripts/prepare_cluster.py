#!/usr/bin/env python3
"""
Pre-cluster symbols and prepare efficient batches
Version that directly reads raw data from DuckDB
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
        """Read data from DuckDB and build in-memory graph"""
        print(f"Loading graph data from {db_file}...")
        con = duckdb.connect(db_file, read_only=True)

        # Load symbol definitions (id -> details), exclude symbols in contrib/
        self.symbol_details = {
            row[0]: {
                'id': row[0],
                'symbol_name': row[1],
                'file_path': row[2],
                'line_num_start': row[3],
                'line_num_end': row[4],
                'symbol_type': row[7]
            } for row in con.execute("SELECT * FROM symbol_definitions where (symbol_type = 'f' OR symbol_type = 's' OR symbol_type = 'v') AND NOT starts_with(file_path, 'contrib/')").fetchall()
        }
        self.all_nodes = set(self.symbol_details.keys())
        print(f"Loaded {len(self.symbol_details)} symbol definitions.")

        # Build graph from reference relationships
        references = con.execute("SELECT from_node, to_node FROM symbol_reference").fetchall()
        con.close()

        self.adj = defaultdict(set)  # Dependencies (which nodes this node depends on)
        self.rev_adj = defaultdict(set) # Dependents (which nodes depend on this node)

        for from_node, to_node in references:
            if from_node in self.all_nodes and to_node in self.all_nodes:
                self.adj[from_node].add(to_node)
                self.rev_adj[to_node].add(from_node)
        print(f"Built graph with {len(references)} references.")


    def init_database(self):
        """Initialize output DuckDB database"""
        # Symbol information table (primary key changed to id)
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

    # Insert data
        self.populate_initial_data()

    def populate_initial_data(self):
        """
        Insert initial data into DuckDB
        """
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

        # Insert dependency relationships
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
        """
        Analyze dependencies in an in-memory graph and create layers using topological sort
        """
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
                
                # Decrease the in-degree of nodes that this node depends on
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

    # Update layer information in the DB
        for i, layer in enumerate(layers):
            for node_id in layer:
                self.meta_db.execute("UPDATE symbols SET layer = ? WHERE id = ?", (i, node_id))
        
        self.meta_db.commit()
        self.layers = layers
        return layers

    def create_file_based_clusters(self):
        """
        Cluster symbols based on files
        """
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
            if len(symbols) <= 3:
                cluster_id_counter += 1
                self.save_cluster(cluster_id_counter, 'file', symbols)
            else:
                # Split by type
                for symbol_type in ['f', 's', 'v']:
                    typed_symbols = [s for s in symbols if s['type'] == symbol_type]
                    if not typed_symbols: continue
                    for i in range(0, len(typed_symbols), 5):
                        cluster_id_counter += 1
                        batch = typed_symbols[i:i+5]
                        self.save_cluster(cluster_id_counter, f'file_{symbol_type}', batch)
        
        self.meta_db.commit()
        return cluster_id_counter

    def save_cluster(self, cluster_id: int, cluster_type: str, symbols: List[Dict]):
        """
        Save cluster to DB (ID-based)
        """
        symbol_ids = [s['id'] for s in symbols]
    # Ensure symbols is not empty
        if not symbols:
            return
    # Handle case where layer is None
        valid_layers = [s.get('layer') for s in symbols if s.get('layer') is not None]
        avg_layer = sum(valid_layers) // len(valid_layers) if valid_layers else 0

        self.meta_db.execute("""
            INSERT INTO clusters (cluster_id, cluster_type, layer, symbols, estimated_tokens)
            VALUES (?, ?, ?, ?, ?)
        """, (
            cluster_id,
            cluster_type,
            avg_layer,
            json.dumps(symbol_ids),  # Save list of IDs as JSON
            len(symbol_ids) * 3000  # Estimated token count
        ))

    # Set cluster ID for symbols
        for symbol in symbols:
            self.meta_db.execute("UPDATE symbols SET cluster_id = ? WHERE id = ?", (cluster_id, symbol['id']))

    def get_symbol_module(self, symbol_id: int) -> str:
        """
        Get module from symbol ID
        """
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
        """
        Generate processing batches
        """
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
                'symbol_ids': symbol_ids, # Changed key to 'symbol_ids' for clarity
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
    db_file = 'global_symbols.db'
    
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