#!/usr/bin/env python3
"""
Main processing for documentation generation using the Gemini Pro model.
This script orchestrates the process of generating documentation for PostgreSQL
symbols by interacting with the Gemini CLI. It manages data using DuckDB,
where symbol information is stored and generated documents are saved.
This version includes controls for both RPM (Requests Per Minute) and
RPD (Requests Per Day) limits to ensure stable, long-running execution.
"""
import json
import duckdb
import subprocess
import time
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Set, Tuple

class DocumentationOrchestrator:
    """
    Orchestrates the AI-driven documentation generation process.
    """
    # Define API limits as class constants.
    RPM_LIMIT = 10  # Requests Per Minute for Gemini 1.5 Pro free tier.
    RPD_LIMIT = 250 # Requests Per Day for Gemini 1.5 Pro free tier.

    def __init__(self, global_symbols_db: str = 'global_symbols.db'):
        self.retry_attempts = 0        
        # Load processing batches which are based on symbol IDs.
        with open('data/processing_batches.json') as f:
            self.batches = json.load(f)

        # Load symbol details into memory for quick access.
        self._load_symbol_details(global_symbols_db)
        
        # Initialize DuckDB connections for metadata and documents.
        self.init_databases()
        
        # Initialize statistics for tracking progress.
        self.stats = {
            'total_batches': len(self.batches),
            'processed_batches': 0,
            'failed_batches': 0,
            'total_symbols': sum(len(b['symbol_ids']) for b in self.batches),
            'processed_symbols': 0
        }

        # ### RPD Limit Control ###
        # Initialize attributes for daily usage tracking.
        self.usage_date = None
        self.request_count = 0

    def _load_symbol_details(self, db_file: str):
        """
        Caches symbol details from global_symbols.db into memory.
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
        Initializes the necessary DuckDB databases and tables.
        """
        # Connection to the existing metadata database.
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        
        # Connection to the database for storing generated documents.
        self.doc_db = duckdb.connect('data/documents.duckdb')
        
        # Document table, using symbol_id as the primary key.
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
        
        # Processing log table, using batch_id as the primary key.
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

        # ### RPD Limit Control ###
        # Table to persist the daily request count.
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS daily_usage (
                id INTEGER PRIMARY KEY DEFAULT 1,
                usage_date VARCHAR,
                request_count INTEGER
            );
        """)

    def _load_or_reset_daily_usage(self):
        """
        Loads the daily request count from the DB or resets it if it's a new day.
        """
        today_str = date.today().isoformat()
        
        result = self.doc_db.execute("SELECT usage_date, request_count FROM daily_usage WHERE id = 1").fetchone()

        if result:
            stored_date, stored_count = result
            if stored_date == today_str:
                # Same day, continue with the stored count.
                self.usage_date = stored_date
                self.request_count = stored_count
                print(f"Resuming with today's request count: {self.request_count}/{self.RPD_LIMIT}")
            else:
                # New day, reset the count.
                self.usage_date = today_str
                self.request_count = 0
                self.doc_db.execute("UPDATE daily_usage SET usage_date = ?, request_count = ? WHERE id = 1", (self.usage_date, self.request_count))
                self.doc_db.commit()
                print("New day detected. Daily request count has been reset.")
        else:
            # First run, initialize the tracking row.
            self.usage_date = today_str
            self.request_count = 0
            self.doc_db.execute("INSERT INTO daily_usage (id, usage_date, request_count) VALUES (1, ?, ?)", (self.usage_date, self.request_count))
            self.doc_db.commit()
            print("Initialized daily request count tracking.")

    def _increment_request_count(self):
        """
        Increments the daily request count in memory and in the database.
        """
        self.request_count += 1
        self.doc_db.execute("UPDATE daily_usage SET request_count = ? WHERE id = 1", (self.request_count,))
        self.doc_db.commit()


    def get_processed_symbol_ids(self) -> Set[int]:
        """
        Retrieves a set of symbol IDs that have already been processed.
        """
        result = self.doc_db.execute("SELECT symbol_id FROM documents").fetchall()
        return set(row[0] for row in result)
        
    def process_all_batches(self):
        """
        Processes all batches sequentially, adhering to RPM and RPD rate limits.
        """
        # The target interval in seconds to adhere to the RPM limit.
        TARGET_INTERVAL_SECONDS = 60.0 / self.RPM_LIMIT

        # ### RPD Limit Control ###
        # Load or reset the daily usage count at the start.
        self._load_or_reset_daily_usage()

        processed_ids = self.get_processed_symbol_ids()
        
        for batch in self.batches:
            # ### RPD Limit Control ###
            # Check the daily limit before starting a new batch.
            if self.request_count >= self.RPD_LIMIT:
                print("\nDaily request limit (RPD) has been reached.")
                print(f"Stopping processing for today. Count: {self.request_count}/{self.RPD_LIMIT} for date {self.usage_date}")
                print("You can safely restart the script tomorrow to continue.")
                break # Exit the main processing loop.

            # Record the start time of the batch processing for RPM control.
            batch_start_time = time.monotonic()

            batch_id = batch['batch_id']
            unprocessed_ids = [sid for sid in batch['symbol_ids'] if sid not in processed_ids]
            if not unprocessed_ids:
                print(f"Batch {batch_id}: All symbols already processed, skipping.")
                continue
                
            print(f"\n{'='*60}")
            print(f"Processing batch {batch_id}/{len(self.batches)}")
            print(f"Layer: {batch['layer']}, Symbols: {len(unprocessed_ids)}")
            print(f"Type: {batch['type']}, Estimated tokens: {batch['estimated_tokens']}")
            print(f"{'='*60}")
            
            # Process the current batch.
            success = self.process_batch(batch, unprocessed_ids)
            
            if success:
                self.stats['processed_batches'] += 1
                self.stats['processed_symbols'] += len(unprocessed_ids)
                processed_ids.update(unprocessed_ids)
            else:
                self.stats['failed_batches'] += 1
                
            self.show_progress()
            
            # Dynamically calculate wait time to adhere to the RPM limit.
            execution_time = time.monotonic() - batch_start_time
            print(f"Batch processing took {execution_time:.2f} seconds.")

            sleep_duration = TARGET_INTERVAL_SECONDS - execution_time
            
            if sleep_duration > 0:
                print(f"Waiting for {sleep_duration:.2f} seconds to maintain a rate of {self.RPM_LIMIT} RPM...")
                time.sleep(sleep_duration)
            else:
                print("Execution time exceeded the target interval. Proceeding immediately.")
            
    def process_batch(self, batch: Dict, symbol_ids: List[int]) -> bool:
        """
        Processes a single batch by invoking the Gemini CLI.
        """
        batch_id = batch['batch_id']
        
        self.doc_db.execute("""
            INSERT OR REPLACE INTO processing_log 
            (batch_id, symbol_ids, status, started_at, processed_count)
            VALUES (?, ?, 'processing', ?, 0)
        """, (batch_id, json.dumps(symbol_ids), datetime.now()))
        self.doc_db.commit()

        prompt, _ = self.build_prompt(symbol_ids, batch['layer'])
        
        try:
            # ### RPD Limit Control ###
            # Increment the request count right before making the API call.
            print(f"Daily request count: {self.request_count + 1}/{self.RPD_LIMIT}")
            self._increment_request_count()

            print("Invoking Gemini CLI...")
            result = subprocess.run(
                [
                    'gemini', '-p', prompt,
                    '--model', 'gemini-2.5-flash', '--yolo',
                ],
                timeout=3600,
                cwd=str(Path.cwd())
            )
            
            if result.returncode == 0:
                print(f"✓ Successfully processed batch {batch_id}")
                
                self.doc_db.execute("""
                    UPDATE processing_log SET status = 'completed', completed_at = ?, processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbol_ids), batch_id))
                time.sleep(1)
                self.store_generated_documents(symbol_ids, batch['layer'])
                return True
            else:
                error_msg = result.stderr[:1000] if result.stderr else 'Unknown error'
                print(f"✗ Error processing batch {batch_id}: {error_msg}")

                if "rate limit" in error_msg.lower() or "429" in error_msg:
                    self.retry_attempts += 1
                    self.doc_db.execute("""
                    UPDATE processing_log SET status = 'rate_limit_error', completed_at = ?, error_message = ?
                    WHERE batch_id = ?;
                    """, (datetime.now(), error_msg, batch_id))                
                    
                    if self.retry_attempts <= 5:
                        wait_time = 60 * (2 ** self.retry_attempts)
                        print(f"Rate limit reached. Waiting for {wait_time} seconds before retry (Attempt {self.retry_attempts}/5)...")
                        time.sleep(wait_time)
                        
                        # We must decrement the count before retrying, as this attempt failed
                        # and the retry logic will increment it again.
                        self.request_count -= 1
                        
                        ret = self.process_batch(batch, symbol_ids)
                        if ret:
                            self.retry_attempts = 0
                        return ret
                    else:
                        print("Max retry attempts reached. Failing this batch.")
                        self.retry_attempts = 0
                        return False
                else:
                    self.doc_db.execute("""
                    UPDATE processing_log SET status = 'error', completed_at = ?, error_message = ?
                    WHERE batch_id = ?;
                    """, (datetime.now(), error_msg, batch_id))
                    return False
                
        except subprocess.TimeoutExpired:
            print(f"✗ Batch {batch_id} timed out.")
            self.doc_db.execute("""
                UPDATE processing_log SET status = 'timeout', completed_at = ? WHERE batch_id = ?;
            """, (datetime.now(), batch_id))
            return False
            
        except Exception as e:
            error_msg = str(e)[:1000]
            print(f"✗ Unexpected error in batch {batch_id}: {error_msg}")
            self.doc_db.execute("""
            UPDATE processing_log SET status = 'error', completed_at = ?, error_message = ?
            WHERE batch_id = ?;
            """, (datetime.now(), error_msg, batch_id))
            return False
        finally:
            self.doc_db.commit()

    def get_processed_summaries(self) -> Dict[str, str]:
        """
        Gets summaries of already processed symbols to provide context.
        """
        result = self.doc_db.execute("""
            SELECT symbol_name, summary FROM documents WHERE summary IS NOT NULL AND summary != ''
            LIMIT 2000;
        """).fetchall()
        return {row[0]: row[1] for row in result}

    def build_prompt(self, symbol_ids: List[int], layer: int) -> Tuple[str, List[str]]:
        """
        Builds the prompt for the AI model for batch processing.
        """
        symbol_names = [self.symbol_details[sid]['name'] for sid in symbol_ids]
        symbol_list_str = '\n'.join([f'- {name}' for name in symbol_names])

        processed_summaries = self.get_processed_summaries()
        
        relevant_processed = set()
        for symbol_id in symbol_ids:
            deps = self.meta_db.execute("""
                SELECT to_node FROM dependencies WHERE from_node = ?;
            """, (symbol_id,)).fetchall()
            
            for (dep_id,) in deps:
                dep_name = self.symbol_details.get(dep_id, {}).get('name')
                if dep_name and dep_name in processed_summaries:
                    summary = processed_summaries[dep_name]
                    relevant_processed.add(f"- {dep_name}: {summary[:120]}")
                    
        relevant_list_str = '\n'.join(sorted(list(relevant_processed))[:15])
        
        prompt = f"""# PostgreSQL Codebase Documentation Generation Task
You are an expert AI assistant specializing in PostgreSQL source code analysis.
Your task is to generate detailed documentation for a given list of symbols by executing command-line tools to gather information.
## Target Symbol List for Processing
{symbol_list_str}
## Summaries of Related Processed Symbols
Below are summaries of already processed symbols that the current symbols may depend on. Use these for contextual understanding.
{relevant_list_str if relevant_list_str else '(No specific related information)'}
## Instructions
1.  For each symbol in the "Target Symbol List for Processing", you MUST use the provided tools to gather information.
2.  Analyze the symbol's source code, definition, and reference locations by executing the `scripts/mcp_tool.py` commands.
3.  Based on the gathered information, generate comprehensive documentation for each symbol in the specified Markdown format.
4.  After generating the documentation for a symbol, you MUST use the `return_document` tool to save it.
## Available Tools
You have access to a shell environment. Use the following commands to interact with the PostgreSQL codebase index and to save your work.
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
# IMPORTANT: Save the generated document using this command.
# The content should be a single string argument.
python3 scripts/mcp_tool.py return_document [symbol_name] "[Markdown Content Here]"
```
All commands return results in JSON format to standard output. You must call return_document for each symbol you document.

Output Markdown Format
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
Now, begin processing the symbols."""
        
        return prompt, symbol_names

    def store_generated_documents(self, symbol_ids: List[int], layer: int):
        """
        Stores generated documents from the temp directory into the DuckDB database.
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

                self.doc_db.execute("""
                    INSERT INTO documents (symbol_id, symbol_name, symbol_type, layer, content, summary, dependencies, related_symbols)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        content = EXCLUDED.content, summary = EXCLUDED.summary,
                        dependencies = EXCLUDED.dependencies, related_symbols = EXCLUDED.related_symbols;
                """, (sid, symbol_name, symbol_type, layer, content, summary, json.dumps(deps), json.dumps(related)))
                
                # doc_path.unlink()
                print(f"  Stored document for: {symbol_name} (ID: {sid})")
            else:
                print(f"  Warning: Document file not found for {symbol_name}")
                
        self.doc_db.commit()

    def extract_summary(self, content: str) -> str:
        """
        Extracts a brief summary from the 'Overview' section of the document.
        """
        import re
        match = re.search(r"##\s*Overview\s*\n+([^#]*)", content, re.IGNORECASE)
        if match:
            summary_text = match.group(1).strip()
            # Take the first two non-empty lines for the summary.
            summary_lines = [line for line in summary_text.split('\n') if line.strip()]
            return ' '.join(summary_lines[:2])
        return ""


    def extract_relationships(self, content: str) -> tuple:
        """
        Extracts dependency and reference relationships from the document content.
        """
        import re
        deps_match = re.search(r"-\s*Functions called/Symbols referenced:\s*\n(.*?)(?=\n-|\n##|\Z)", content, re.DOTALL)
        deps_list = re.findall(r'-\s*(\w+)', deps_match.group(1) if deps_match else '')

        related_match = re.search(r'-\s*Called from \(representative examples\):\s*\n(.*?)(?=\n-|\n##|\Z)', content, re.DOTALL)
        related_list = re.findall(r'-\s*(\w+)', related_match.group(1) if related_match else '')

        return list(set(deps_list)), list(set(related_list))

    def show_progress(self):
        """
        Displays the current progress of the documentation generation task.
        """
        if self.stats['total_symbols'] == 0: return
        processed_ids = self.get_processed_symbol_ids()
        progress = (float(len(processed_ids)) / self.stats['total_symbols']) * 100
        print(f"\nProgress: {progress:.1f}% ({len(processed_ids)}/{self.stats['total_symbols']})")
        print(f"Completed Batches: {self.stats['processed_batches']}/{self.stats['total_batches']}")

def main():
    """
    Main function to run the documentation orchestrator.
    """
    orchestrator = DocumentationOrchestrator()
    print("PostgreSQL Documentation Generation Orchestrator (Gemini - ID-based)")
    print("=" * 60)
    orchestrator.process_all_batches()
    print("\n" + "=" * 60)
    print("Documentation generation completed or paused due to daily limit.")


main()