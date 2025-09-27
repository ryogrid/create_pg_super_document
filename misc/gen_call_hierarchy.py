#!/usr/bin/env python3
"""
Generate function call hierarchy for PostgreSQL codebase.
Creates a list of functions ordered by their call hierarchy using BFS from main entry points.
"""

import duckdb
import os
from collections import defaultdict, deque
from typing import Set, Dict, List, Tuple

def load_function_symbols(conn_global, conn_docs) -> Dict[int, str]:
    """Load all function symbols from documents table that have symbol_type 'f'."""
    # Get all symbol_ids from documents table
    doc_symbols = conn_docs.execute("""
        SELECT DISTINCT symbol_id
        FROM documents
    """).fetchall()

    if not doc_symbols:
        print("Warning: No symbols found in documents table")
        return {}

    # Filter for function symbols (symbol_type = 'f')
    symbol_ids = [row[0] for row in doc_symbols]
    placeholders = ','.join(['?' for _ in symbol_ids])

    function_symbols = conn_global.execute(f"""
        SELECT id, symbol_name
        FROM symbol_definitions
        WHERE id IN ({placeholders})
        AND symbol_type = 'f'
    """, symbol_ids).fetchall()

    return {sym_id: name for sym_id, name in function_symbols}

def load_call_relationships(conn_global, function_ids: Set[int]) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
    """Load call relationships for the given function IDs."""
    calls_from = defaultdict(list)  # function -> list of functions it calls
    calls_to = defaultdict(list)    # function -> list of functions that call it

    if not function_ids:
        return calls_from, calls_to

    # Get all references between functions
    func_list = list(function_ids)
    placeholders = ','.join(['?' for _ in func_list])

    references = conn_global.execute(f"""
        SELECT from_node, to_node
        FROM symbol_reference
        WHERE from_node IN ({placeholders})
        AND to_node IN ({placeholders})
    """, func_list + func_list).fetchall()

    for from_node, to_node in references:
        if from_node != to_node:  # Avoid self-references
            calls_from[from_node].append(to_node)
            calls_to[to_node].append(from_node)

    return calls_from, calls_to

def detect_cycles(calls_from: Dict[int, List[int]]) -> Set[Tuple[int, int]]:
    """Detect cycles in the call graph and return edges to break."""
    edges_to_break = set()
    visited = set()
    rec_stack = set()

    def dfs(node: int, path: List[int]) -> bool:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        for neighbor in calls_from.get(node, []):
            if neighbor not in visited:
                if dfs(neighbor, path.copy()):
                    return True
            elif neighbor in rec_stack:
                # Found a cycle, break the edge with highest depth
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:]
                if len(cycle) > 1:
                    # Break the edge from the last node to the first
                    edges_to_break.add((path[-1], neighbor))

        rec_stack.remove(node)
        return False

    for node in calls_from:
        if node not in visited:
            dfs(node, [])

    return edges_to_break

def build_hierarchy_bfs(entry_points: List[str], function_symbols: Dict[int, str],
                        calls_from: Dict[int, List[int]], calls_to: Dict[int, List[int]]) -> List[int]:
    """Build function hierarchy using BFS from entry points."""
    # Find IDs for entry points
    entry_ids = []
    symbol_to_id = {name: sym_id for sym_id, name in function_symbols.items()}

    for entry in entry_points:
        if entry in symbol_to_id:
            entry_ids.append(symbol_to_id[entry])
        else:
            print(f"Warning: Entry point '{entry}' not found in function symbols")

    if not entry_ids:
        print("Warning: No entry points found in function symbols")
        return []

    # BFS to build hierarchy
    visited = set()
    hierarchy = []
    queue = deque()

    # Start with entry points at level 0
    for entry_id in entry_ids:
        if entry_id not in visited:
            queue.append((entry_id, 0))
            visited.add(entry_id)

    level_nodes = defaultdict(list)

    while queue:
        node_id, level = queue.popleft()
        level_nodes[level].append(node_id)

        # Add called functions to queue
        for called_id in calls_from.get(node_id, []):
            if called_id not in visited:
                visited.add(called_id)
                queue.append((called_id, level + 1))

    # Build hierarchy from level_nodes
    for level in sorted(level_nodes.keys()):
        hierarchy.extend(level_nodes[level])

    return hierarchy, visited

def add_remaining_connected(function_symbols: Dict[int, str], visited: Set[int],
                           calls_from: Dict[int, List[int]], calls_to: Dict[int, List[int]]) -> List[int]:
    """Add functions that weren't reached from entry points but are connected to the graph."""
    additional = []
    remaining = set(function_symbols.keys()) - visited

    for func_id in remaining:
        # Check if this function is connected to any visited function
        connected = False

        # Check outgoing connections
        for called_id in calls_from.get(func_id, []):
            if called_id in visited:
                connected = True
                break

        # Check incoming connections
        if not connected:
            for caller_id in calls_to.get(func_id, []):
                if caller_id in visited:
                    connected = True
                    break

        if connected:
            additional.append(func_id)
            visited.add(func_id)

    return additional

def main():
    # Database paths
    global_db_path = "global_symbols.db"
    docs_db_path = "data/documents.duckdb"

    # Check if databases exist
    if not os.path.exists(global_db_path):
        print(f"Error: {global_db_path} not found")
        return

    if not os.path.exists(docs_db_path):
        print(f"Error: {docs_db_path} not found")
        return

    # Connect to databases
    conn_global = duckdb.connect(global_db_path, read_only=True)
    conn_docs = duckdb.connect(docs_db_path, read_only=True)

    try:
        print("Loading function symbols...")
        function_symbols = load_function_symbols(conn_global, conn_docs)
        print(f"Found {len(function_symbols)} function symbols")

        if not function_symbols:
            print("No function symbols found, exiting")
            return

        print("Loading call relationships...")
        calls_from, calls_to = load_call_relationships(conn_global, set(function_symbols.keys()))

        print("Detecting and breaking cycles...")
        edges_to_break = detect_cycles(calls_from)

        # Remove edges that cause cycles
        for from_node, to_node in edges_to_break:
            if from_node in calls_from:
                calls_from[from_node] = [n for n in calls_from[from_node] if n != to_node]
            if to_node in calls_to:
                calls_to[to_node] = [n for n in calls_to[to_node] if n != from_node]

        print(f"Removed {len(edges_to_break)} edges to break cycles")

        # Entry points for BFS
        entry_points = [
            "PostmasterMain",
            "BackendMain",
            "WalWriterMain",
            "WalReceiverMain",
            "CheckpointerMain",
            "BackgroundWriterMain",
            "SysLoggerMain"
        ]

        print(f"Building hierarchy from entry points: {', '.join(entry_points)}")
        hierarchy, visited = build_hierarchy_bfs(entry_points, function_symbols, calls_from, calls_to)
        print(f"Hierarchy contains {len(hierarchy)} functions")

        print("Adding remaining connected functions...")
        additional = add_remaining_connected(function_symbols, visited, calls_from, calls_to)
        hierarchy.extend(additional)
        print(f"Added {len(additional)} connected functions")

        # Add isolated functions (no calls in or out)
        isolated = []
        for func_id in function_symbols:
            if func_id not in visited:
                isolated.append(func_id)

        hierarchy.extend(isolated)
        print(f"Added {len(isolated)} isolated functions")

        # Write output
        output_dir = "experimental"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "function_call_hierarchy.txt")

        with open(output_path, 'w') as f:
            for func_id in hierarchy:
                if func_id in function_symbols:
                    f.write(f"{function_symbols[func_id]}\n")

        print(f"\nOutput written to {output_path}")
        print(f"Total functions in hierarchy: {len(hierarchy)}")

    finally:
        conn_global.close()
        conn_docs.close()

if __name__ == "__main__":
    main()