#!/usr/bin/env python3
"""
Main processing for documentation generation using Claude Code.
DuckDB version - Documents are also stored in the DB (ID-based processing).
"""
import json
import duckdb
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple

class DocumentationOrchestrator:
    def __init__(self, global_symbols_db: str = 'global_symbols.db'):
        self.retry_attempts = 0        
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
        """
        Cache symbol details from global_symbols.db into memory
        """
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
        """
        Initialize DuckDB databases
        """
        # Metadata DB (existing)
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        
        # Document-specific DB
        self.doc_db = duckdb.connect('data/documents.duckdb')
        
        # Document table (revised to be ID-based)
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
            );
        """)
        
        # Processing log table (batch_id as primary key)
        # Since metadata.duckdb is opened as read-only, logs are written to doc_db
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                batch_id INTEGER PRIMARY KEY,
                symbol_ids JSON,
                status VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                processed_count INTEGER
            );
        """)

    def get_processed_symbol_ids(self) -> Set[int]:
        """
        Get processed symbol IDs
        """
        result = self.doc_db.execute("SELECT symbol_id FROM documents").fetchall()
        return set(row[0] for row in result)
        
    def process_all_batches(self):
        """
        Process all batches sequentially
        """
        processed_ids = self.get_processed_symbol_ids()
        
        for batch in self.batches:
            batch_id = batch['batch_id']
            # Skip check (ID-based)
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
            
            # Rate limiting countermeasure
            time.sleep(45)
            
    def process_batch(self, batch: Dict, symbol_ids: List[int]) -> bool:
        """
        Process a single batch
        """
        batch_id = batch['batch_id']
        
        # Start log recording
        self.doc_db.execute("""
            INSERT OR REPLACE INTO processing_log 
            (batch_id, symbol_ids, status, started_at, processed_count)
            VALUES (?, ?, 'processing', ?, 0)
        """, (batch_id, json.dumps(symbol_ids), datetime.now()))
        self.doc_db.commit()

        # Build prompt
        prompt, symbols = self.build_prompt(symbol_ids, batch['layer'])
        
        try:
            # Run Claude Code
            print("Invoking Claude Code CLI...")
            result = subprocess.run(
                [
                    'claude', '--allowedTools', 'Bash(python3*),Read', '-p', f"{prompt}",
                    '--model', 'claude-sonnet-4-20250514',
                    '--max-turns', str(min(len(symbols) * 20, 80)),
                    '--permission-mode', 'bypassPermissions',
                ],
                capture_output=True,
                text=True,
                timeout=3600,
                cwd=str(Path.cwd())
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully processed batch {batch_id}")
                
                # Log success
                self.doc_db.execute("""
                    UPDATE processing_log SET status = 'completed', completed_at = ?, processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbol_ids), batch_id))

                time.sleep(1)

                # Here, instead of parsing directly, assume the agent outputs to a file
                self.store_generated_documents(symbol_ids, batch['layer'])
                
                return True
            else:
                error_msg = result.stderr[:1000] if result.stderr else 'Unknown error'
                if error_msg == "Unknown error":
                    self.retry_attempts += 1
                    self.doc_db.execute("""
                    UPDATE processing_log SET status = 'error', completed_at = ?, error_message = ?
                    WHERE batch_id = ?;
                    """, (datetime.now(), error_msg, batch_id))                
                    # wait 10 seconds and retry up to 12 times (2 hours) for rate limit reached case
                    if self.retry_attempts <= 12:
                        print("wait 10 minutes before retry...")
                        time.sleep(600)  # Wait 10 minutes before retry
                        print(f"Retrying batch {batch_id} (Attempt {retry_attempts}/12)...")
                        # retry
                        ret = self.process_batch(batch, symbol_ids)
                        retry_attempts = 0
                        return ret                                    
                
        except subprocess.TimeoutExpired:
            print(f"✗ Batch {batch_id} timed out")
            self.doc_db.execute("""
                UPDATE processing_log SET status = 'timeout', completed_at = ? WHERE batch_id = ?;
            """, (datetime.now(), batch_id))
            return False
            
        except Exception as e:
            error_msg = str(e)[:1000]
            print(f"✗ Unexpected error in batch {batch_id}: {error_msg}")
            if error_msg == "Unknown error":
                retry_attempts += 1
                self.doc_db.execute("""
                UPDATE processing_log SET status = 'error', completed_at = ?, error_message = ?
                WHERE batch_id = ?;
                """, (datetime.now(), error_msg, batch_id))                
                # wait 10 seconds and retry up to 12 times (2 hours) for rate limit reached case
                if self.retry_attempts <= 12:
                    print("wait 10 minutes before retry...")
                    time.sleep(600)  # Wait 10 minutes before retry
                    print(f"Retrying batch {batch_id} (Attempt {retry_attempts}/12)...")
                    # retry
                    ret = self.process_batch(batch, symbol_ids)
                    retry_attempts = 0
                    return ret

            return False
        finally:
            self.doc_db.commit()

    def get_processed_summaries(self) -> Dict[str, str]:
        """
        Get summaries of processed symbols (name -> summary)
        """
        result = self.doc_db.execute("""
            SELECT symbol_name, summary FROM documents WHERE summary IS NOT NULL AND summary != ''
            LIMIT 2000;
        """).fetchall()
        return {row[0]: row[1] for row in result}

    def build_prompt(self, symbol_ids: List[int], layer: int) -> Tuple[str, List[str]]:
        """
        Build prompt for batch processing
        """
        symbol_names = [self.symbol_details[sid]['name'] for sid in symbol_ids]
        symbol_list_str = '\n'.join([f'- {name}' for name in symbol_names])

        processed_summaries = self.get_processed_summaries()
        
        relevant_processed = set()
        for symbol_id in symbol_ids:
            # Get dependencies for this symbol (ID-based)
            deps = self.meta_db.execute("""
                SELECT to_node FROM dependencies WHERE from_node = ?;
            """, (symbol_id,)).fetchall()
            
            for (dep_id,) in deps:
                dep_name = self.symbol_details.get(dep_id, {}).get('name')
                if dep_name and dep_name in processed_summaries:
                    summary = processed_summaries[dep_name]
                    relevant_processed.add(f"- {dep_name}: {summary[:120]}")
                    
        relevant_list_str = '\n'.join(sorted(list(relevant_processed))[:15])
        
        # Prompt template
        # Prompt assumes referencing index with claude command
        prompt = f"""# PostgreSQL Codebase Documentation Generation Task

You are an expert deeply familiar with PostgreSQL source code.
Reference the entire indexed PostgreSQL codebase and generate detailed documentation for the following unprocessed symbols.

## Target Symbol List for Processing
{symbol_list_str}

## Summaries of Related Processed Symbols
Below are summaries of already processed symbols that the current symbols may depend on. Use these for contextual understanding.
{relevant_list_str if relevant_list_str else '(No specific related information)'}

## Instructions
1. Process each symbol in the "Target Symbol List for Processing" above, in order.
2. Search and analyze each symbol's source code, definition, and reference locations with execution of scripts/mcp_tool.py.
3. Generate documentation for each symbol following the Markdown format below.
4. Save the generated document with execution of scripts/mcp_tool.py.

## Available Tools

Access PostgreSQL symbol information and return generated documents using these command-line tools:

```bash
# Get symbol details (type, file, line numbers, etc.)
python3 scripts/mcp_tool.py get_symbol_details [symbol_name]

# Get source code
python3 scripts/mcp_tool.py get_symbol_source [symbol_name]

# Get symbols referenced by this symbol
python3 scripts/mcp_tool.py get_references_from_this [symbol_name]

# Get symbols that reference this symbol
python3 scripts/mcp_tool.py get_references_to_this [symbol_name]

# Search symbols by pattern
python3 scripts/mcp_tool.py search_symbols [pattern]

# Return generated document
python3 scripts/mcp_tool.py return_document [symbol_name] [content]
```
All commands return results in JSON format.

## Output Markdown Format
```markdown
# [Symbol Name]

## Overview
(Briefly explain the purpose and role of this symbol in 1-2 sentences)

## Definition
(Provide the function signature or struct/enum definition)
Example: void InitPostgres(const char *in_dbname, Oid dboid, const char *username, Oid useroid, char *out_dbname)


## Detailed Description
(Provide specific explanation of the symbol's functionality, behavior, design philosophy, etc.)

## Parameters / Member Variables
(Explain the role and meaning of each function parameter or struct member in a bulleted list)
- `param1`: (description)
- `member1`: (description)

## Dependencies
- Functions called/Symbols referenced:
  - func_a
  - TYPE_B
- Called from (representative examples):
  - caller_func_x
  - caller_func_y

## Notes and Other Information
(Notable points, usage precautions, related background knowledge, etc.)

```
"""
        return (prompt, symbol_names)
        
    def store_generated_documents(self, symbol_ids: List[int], layer: int):
        """
        Store generated documents in DuckDB
        """
        temp_dir = Path('output/temp')
        temp_dir.mkdir(exist_ok=True)
        
        for sid in symbol_ids:
            symbol_name = self.symbol_details[sid]['name']
            symbol_type = self.symbol_details[sid]['type']
            doc_path = temp_dir / f"{symbol_name}.md"
            
            if doc_path.exists():
                content = doc_path.read_text(encoding='utf-8')
                summary = self.extract_summary(content)
                try:
                    deps, related = self.extract_relationships(content)
                except Exception as e:
                    print(f"  Error extracting relationships for {symbol_name}: {e}")
                    deps, related = [], []

                # Store document in DB (ID-based)
                self.doc_db.execute("""
                    INSERT INTO documents (symbol_id, symbol_name, symbol_type, layer, content, summary, dependencies, related_symbols)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        content = EXCLUDED.content, summary = EXCLUDED.summary,
                        dependencies = EXCLUDED.dependencies, related_symbols = EXCLUDED.related_symbols;
                """, (sid, symbol_name, symbol_type, layer, content, summary, json.dumps(deps), json.dumps(related)))
                
                doc_path.unlink() # Delete temporary file
                print(f"  Stored document for: {symbol_name} (ID: {sid})")
            else:
                print(f"  Warning: Document file not found for {symbol_name}")
                
        self.doc_db.commit()

    def extract_summary(self, content: str) -> str:
        """
        Extract summary from document
        """
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
        """
        Extract relationships from document
        """
        import re
        deps = re.findall(r'-\s*Functions called/Symbols referenced:\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        deps_list = re.findall(r'-\s*(\w+)', ''.join(deps))

        related = re.findall(r'-\s*Called from \(representative examples\):\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        related_list = re.findall(r'-\s*(\w+)', ''.join(related))

        return list(set(deps_list)), list(set(related_list))

    def show_progress(self):
        """
        Show progress
        """
        if self.stats['total_symbols'] == 0: return
        processed_ids = self.get_processed_symbol_ids()
        progress = (float(len(processed_ids)) / self.stats['total_symbols']) * 100
        print(f"\nProgress: {progress:.1f}% ({len(processed_ids)}/{self.stats['total_symbols']})")
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
