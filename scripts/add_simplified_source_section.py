#!/usr/bin/env python3
"""
Parallel function source code simplification using Claude CLI.
Processes PostgreSQL functions to add simplified source code sections to their documentation.
"""

import json
import subprocess
import time
import threading
import queue
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set
import logging
import re

# --- Configuration ---
# Default number of parallel claude commands to run
DEFAULT_MAX_PARALLEL_COMMANDS = 3
# Number of functions to process in a single claude command
FUNCTIONS_PER_COMMAND = 10
# --- End Configuration ---

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simplification_progress.log'),
        logging.StreamHandler()
    ]
)

def get_first_alnum_char(text: str) -> str:
    """
    Returns the first alphanumeric character in a string.
    Returns '_' if no alphanumeric character is found.
    This is case-sensitive.
    """
    for char in text:
        if char.isalnum():
            return char
    return "_" # Default directory if no alphanumeric character is found

class FunctionSimplificationOrchestrator:
    def __init__(self,
                 function_list_file: str = 'experimental/function_call_hierarchy.txt',
                 max_parallel: int = DEFAULT_MAX_PARALLEL_COMMANDS,
                 functions_per_command: int = FUNCTIONS_PER_COMMAND,
                 timeout_seconds: int = 1200):
        """
        Initialize the orchestrator.
        
        Args:
            function_list_file: Path to file containing function names (one per line)
            max_parallel: Number of parallel Claude processes to run
            functions_per_command: Number of functions to process in a single Claude command
            timeout_seconds: Timeout for each Claude invocation
        """
        self.function_list_file = Path(function_list_file)
        self.max_parallel = max_parallel
        self.functions_per_command = functions_per_command
        self.timeout_seconds = timeout_seconds
        
        # Load function list
        self.functions = self._load_function_list()
        
        # Progress tracking
        self.completed = []
        self.failed = []
        self.skipped = []
        self.in_progress = {}
        self.lock = threading.Lock()
        
        # Queue for functions to process
        self.work_queue = queue.Queue()
        for func_info in self.functions:
            self.work_queue.put(func_info)
        
        # Statistics
        self.start_time = None
        self.stats = {
            'total': len(self.functions),
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'in_progress': 0
        }
    
    def _load_function_list(self) -> List[Dict[str, str]]:
        """Load function names from file and generate their corresponding doc paths."""
        if not self.function_list_file.exists():
            logging.error(f"Function list file not found: {self.function_list_file}")
            return []
        
        functions = []
        with open(self.function_list_file, 'r', encoding='utf-8') as f:
            for line in f:
                function_name = line.strip()
                if function_name:
                    doc_path = f'generated_docs/{get_first_alnum_char(function_name)}/{function_name}.md'
                    functions.append({'name': function_name, 'path': doc_path})

        logging.info(f"Loaded and generated paths for {len(functions)} functions from {self.function_list_file}")
        return functions
    
    def _check_simplified_source_exists(self, doc_path_str: str) -> bool:
        """
        Check if simplified source section already exists in the markdown file.
        """
        doc_path = Path(doc_path_str)
        if not doc_path.exists():
            return False
        
        try:
            content = doc_path.read_text(encoding='utf-8')
            return '## Simplified Source' in content
        except Exception:
            return False

    def _build_prompt(self, function_batch: List[Dict[str, str]]) -> str:
        """
        Build the combined prompt for Claude to process a batch of functions.
        """
        functions_json = json.dumps(function_batch, indent=2)

        prompt = f"""# Batch Function Source Code Simplification Task

You will process a batch of {len(function_batch)} PostgreSQL functions based on the provided JSON data.

## Your Task
For each function object in the JSON data below:
1.  Use the `name` key for the function's name and the `path` key for its documentation file path.
2.  Retrieve the function's source code.
3.  Create a simplified, readable version.
4.  Directly append this simplified version to the specified documentation file.
5.  DO NOT create temporary files.
6.  After processing all functions, provide a single JSON array summarizing the outcome for each function.

## Function Batch to Process (JSON format)```json
{functions_json}
```

## Available MCP Server Tools
You have access to the following MCP server functions for PostgreSQL symbol information:

- **pg_symbol_source(symbol_name)**: Retrieve the complete source code for a symbol
- **pg_symbol_overview(symbol_name)**: Get a concise overview/summary of the symbol
- **pg_symbol_document(symbol_name)**: Fetch detailed documentation for the symbol
- **pg_references_from(symbol_name)**: List symbols referenced by the given symbol
- **pg_references_to(symbol_name)**: List symbols that reference the given symbol

## Processing Loop
Iterate through each JSON object in the array above. For each object, perform the simplification task.

### For each `function` object (`{{ "name": "...", "path": "..." }}`):

1.  **Retrieve Source**: Use `pg_symbol_source(function.name)`.
2.  **Simplify Code**: Reduce complexity while preserving essential logic.
Apply these simplification techniques:
- Remove non-essential error handling (keep only critical checks)
- Simplify complex conditions into clearer logic flow
- Replace detailed memory operations with high-level comments
- Use descriptive variable names instead of cryptic ones
- Add brief explanatory comments for complex logic
- Remove platform-specific code, focus on main logic path
- Consolidate similar cases or branches
- Target: 20-50% of original length while preserving essential algorithm

3.  **Update Documentation**:
    - Read the file at `function.path`.
    - If "## Simplified Source" already exists, do nothing for this file.
    - Otherwise, append the new section to the file using the Write tool.

## Important Guidelines at Simplification
- Preserve the essential algorithm and logic flow
- Don't oversimplify to lose important functionality
- Maintain correctness - represent what the function actually does

## Simplification Example

```c
ReturnType FunctionName(Parameters) {{
    // Core logic step 1: Brief description
    simplified_logic_1();

    // Core logic step 2: Brief description
    if (important_condition) {{
        simplified_action();
    }}

    // Additional core logic steps...

    return simplified_result;
}}
```

## Final Status Reporting
After attempting to process ALL functions, you MUST output a single JSON array containing a result object for each function. The output must ONLY be the JSON array, enclosed in ```json ... ```.

### Required JSON Output Format
```json
[
  {{
    "function": "function_name_from_input",
    "status": "COMPLETED"
  }},
  {{
    "function": "another_function_name",
    "status": "NOT_FOUND",
    "details": "Symbol not found using pg_symbol_source."
  }}
]
```

### Possible `status` values:
- `COMPLETED`: Successfully added the simplified source.
- `ALREADY_PROCESSED`: The "## Simplified Source" section was already present.
- `NOT_A_FUNCTION`: The symbol is not a function.
- `NOT_FOUND`: The symbol was not found.
- `MCP_ERROR`: An MCP tool error occurred.
- `WRITE_ERROR`: A file writing error occurred.

Start processing the batch now. Your final output should be only the JSON array.
"""
        return prompt

    def _process_function(self, worker_id: int):
        """
        Worker thread that processes functions from the queue in batches.
        """
        while True:
            try:
                # 1. Get a batch of functions from the queue
                function_batch = []
                try:
                    while len(function_batch) < self.functions_per_command:
                        func_info = self.work_queue.get_nowait()
                        function_batch.append(func_info)
                except queue.Empty:
                    pass

                if not function_batch:
                    if self.work_queue.empty() and not self.in_progress:
                        break
                    time.sleep(1)
                    continue

                # 2. Filter out functions that are already processed
                unprocessed_batch = []
                for func_info in function_batch:
                    if self._check_simplified_source_exists(func_info['path']):
                        with self.lock:
                            self.skipped.append(func_info['name'])
                            self.stats['skipped'] += 1
                        logging.info(f"[Worker {worker_id}] ⊘ Already processed: {func_info['name']}")
                        self.work_queue.task_done()
                    else:
                        unprocessed_batch.append(func_info)
                
                if not unprocessed_batch:
                    continue

                # 3. Process the remaining batch
                with self.lock:
                    self.in_progress[worker_id] = [f['name'] for f in unprocessed_batch]
                    self.stats['in_progress'] += len(unprocessed_batch)
                
                logging.info(f"[Worker {worker_id}] Starting batch of {len(unprocessed_batch)}: {', '.join([f['name'] for f in unprocessed_batch])}")
                
                prompt = self._build_prompt(unprocessed_batch)
                
                try:
                    result = subprocess.run(
                        ['claude', '--allowedTools', 'mcp,Read,Write', '-p', prompt, '--model', 'claude-sonnet-4-20250514', '--permission-mode', 'bypassPermissions'],
                        capture_output=True, text=True, timeout=self.timeout_seconds, cwd=str(Path.cwd()), encoding='utf-8'
                    )
                    
                    output = result.stdout + result.stderr

                    # 4. Check for session/rate limit errors globally
                    if "Session limit reached" in output or "Rate limit exceeded" in output:
                        logging.warning(f"[Worker {worker_id}] ✗ Session/Rate limit hit. Re-queueing batch and sleeping.")
                        with self.lock:
                            for func_info in unprocessed_batch:
                                self.failed.append((func_info['name'], "Session/Rate limit"))
                                self.stats['failed'] += 1
                                self.work_queue.put(func_info) # Requeue
                        time.sleep(3600)
                        continue

                    # 5. Find, parse, and process the JSON output
                    json_results = []
                    try:
                        json_match = re.search(r"```json\s*([\s\S]*?)\s*```", output, re.MULTILINE)
                        if json_match:
                            json_str = json_match.group(1)
                            json_results = json.loads(json_str)
                        elif output.strip().startswith('['): # Fallback for raw JSON
                            json_results = json.loads(output.strip())
                        else:
                            raise json.JSONDecodeError("No JSON block found", output, 0)
                    except json.JSONDecodeError:
                        logging.error(f"[Worker {worker_id}] ✗ Failed to decode JSON for batch. Output: {output[:200]}...")
                        with self.lock:
                            for func_info in unprocessed_batch:
                                self.failed.append((func_info['name'], "Invalid/No JSON output"))
                                self.work_queue.put(func_info) # Requeue
                                self.stats['failed'] += 1
                        continue # Move to the next batch

                    # 6. Record results based on the parsed JSON
                    results_map = {item['function']: item for item in json_results}
                    for func_info in unprocessed_batch:
                        func_name = func_info['name']
                        doc_path = func_info['path']
                        
                        if func_name not in results_map:
                            with self.lock:
                                self.failed.append((func_name, "Missing from result JSON"))
                                self.stats['failed'] += 1
                            logging.warning(f"[Worker {worker_id}] ✗ {func_name} missing from result JSON.")
                            continue

                        res = results_map[func_name]
                        status = res.get('status', 'UNKNOWN')
                        details = res.get('details', '')

                        if status == 'COMPLETED':
                            if self._check_simplified_source_exists(doc_path):
                                with self.lock:
                                    self.completed.append(func_name)
                                    self.stats['completed'] += 1
                                logging.info(f"[Worker {worker_id}] ✓ Completed: {func_name}")
                            else:
                                with self.lock:
                                    self.failed.append((func_name, "File not updated despite 'COMPLETED' status"))
                                    self.stats['failed'] += 1
                                logging.warning(f"[Worker {worker_id}] ✗ Failed (file not updated): {func_name}")
                        elif status in ['ALREADY_PROCESSED', 'NOT_A_FUNCTION']:
                            with self.lock:
                                self.skipped.append(func_name)
                                self.stats['skipped'] += 1
                            logging.info(f"[Worker {worker_id}] ⊘ Skipped ({status}): {func_name}")
                        else: # NOT_FOUND, MCP_ERROR, WRITE_ERROR, etc.
                            with self.lock:
                                self.failed.append((func_name, f"{status}: {details}"))
                                self.stats['failed'] += 1
                            logging.warning(f"[Worker {worker_id}] ✗ Failed ({status}): {func_name}")

                except subprocess.TimeoutExpired:
                    logging.error(f"[Worker {worker_id}] ✗ Timeout on batch: {[f['name'] for f in unprocessed_batch]}")
                    with self.lock:
                        for func_info in unprocessed_batch:
                            self.failed.append((func_info['name'], "Timeout"))
                            self.work_queue.put(func_info) # Requeue
                            self.stats['failed'] += 1
                
                except Exception as e:
                    logging.error(f"[Worker {worker_id}] ✗ Error on batch {[f['name'] for f in unprocessed_batch]}: {e}")
                    with self.lock:
                        for func_info in unprocessed_batch:
                            self.failed.append((func_info['name'], str(e)))
                            self.stats['failed'] += 1
                
                finally:
                    with self.lock:
                        if worker_id in self.in_progress:
                            self.stats['in_progress'] -= len(unprocessed_batch)
                            del self.in_progress[worker_id]
                    
                    for _ in unprocessed_batch:
                        self.work_queue.task_done()
                    
                    total_processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
                    if total_processed % 5 == 0: self._report_progress()
                    if total_processed % 20 == 0: self._report_summary()
                    
                    time.sleep(2)
                    
            except Exception as e:
                logging.error(f"[Worker {worker_id}] Unexpected error in main loop: {e}")
                time.sleep(5)
    
    def _report_progress(self):
        """Report current progress."""
        with self.lock:
            processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
            percentage = (processed / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
            
            logging.info(f"\n{'='*60}")
            logging.info(f"[Progress Update - {processed}/{self.stats['total']} ({percentage:.1f}%)]")
            logging.info(f"Completed: {self.stats['completed']}, Failed: {self.stats['failed']}, Skipped: {self.stats['skipped']}, In Progress: {self.stats['in_progress']}")
            
            if self.completed:
                logging.info(f"Recently completed: {', '.join(self.completed[-5:])}")
            if self.failed:
                recent_failed = self.failed[-3:]
                logging.info("Recent failures:")
                for func, reason in recent_failed: logging.info(f"  ✗ {func}: {reason}")
            logging.info(f"{'='*60}\n")
    
    def _report_summary(self):
        """Report detailed summary."""
        with self.lock:
            elapsed = time.time() - self.start_time if self.start_time else 0
            elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
            
            logging.info(f"\n{'='*70}\n[SUMMARY REPORT]\n{'='*70}")
            processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
            logging.info(f"Total Progress: {processed}/{self.stats['total']}")
            success_denominator = processed - self.stats['skipped']
            if success_denominator > 0:
                logging.info(f"Success Rate: {self.stats['completed']}/{success_denominator} (excluding skipped)")
            logging.info(f"Elapsed Time: {elapsed_str}")
            
            if elapsed > 0 and processed > 0:
                rate = (self.stats['completed'] + self.stats['failed'] * 0.7) / (elapsed / 60)
                logging.info(f"Processing Rate: {rate:.1f} functions/minute")
                remaining = self.stats['total'] - processed
                if rate > 0:
                    eta_minutes = remaining / rate
                    logging.info(f"Estimated Time Remaining: {int(eta_minutes)}m")
            logging.info(f"{'='*70}\n")
    
    def run(self):
        """Run the parallel simplification process."""
        self.start_time = time.time()
        
        logging.info(f"\n{'='*70}")
        logging.info(f"Starting Function Simplification with MCP Server")
        logging.info(f"Total functions: {self.stats['total']}")
        logging.info(f"Parallel workers: {self.max_parallel}")
        logging.info(f"Functions per command: {self.functions_per_command}")
        logging.info(f"Timeout per batch: {self.timeout_seconds}s")
        logging.info(f"{'='*70}\n")
        
        workers = [threading.Thread(target=self._process_function, args=(i,)) for i in range(self.max_parallel)]
        for worker in workers: worker.start()
        for worker in workers: worker.join()
        
        self._final_report()
    
    def _final_report(self):
        """Generate final completion report."""
        elapsed = time.time() - self.start_time
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        
        logging.info(f"\n{'='*70}\n[FINAL REPORT]\n{'='*70}")
        total_processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
        logging.info(f"Total functions processed: {total_processed}/{self.stats['total']}")
        logging.info(f"Successfully simplified: {self.stats['completed']}")
        logging.info(f"Failed: {self.stats['failed']}")
        logging.info(f"Skipped (already processed or not functions): {self.stats['skipped']}")
        logging.info(f"Total time: {elapsed_str}")
        
        success_denominator = total_processed - self.stats['skipped']
        if success_denominator > 0:
            success_rate = (self.stats['completed'] / success_denominator) * 100
            logging.info(f"Success rate (excluding skipped): {success_rate:.1f}%")
        
        if self.failed:
            logging.info("\nFailed functions:")
            for func, reason in self.failed: logging.info(f"  - {func}: {reason}")
        
        results = {
            'completed': self.completed,
            'failed': [{'function': f, 'reason': r} for f, r in self.failed],
            'skipped': self.skipped, 'stats': self.stats, 'elapsed_seconds': elapsed
        }
        
        with open('simplification_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logging.info(f"\nResults saved to simplification_results.json\n{'='*70}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simplify PostgreSQL function source code in parallel using MCP server')
    parser.add_argument('--input', default='experimental/function_call_hierarchy.txt',
                       help='Input file with function names (one per line)')
    parser.add_argument('--parallel', type=int, default=DEFAULT_MAX_PARALLEL_COMMANDS,
                       help=f'Number of parallel Claude processes (default: {DEFAULT_MAX_PARALLEL_COMMANDS})')
    parser.add_argument('--batch-size', type=int, default=FUNCTIONS_PER_COMMAND,
                       help=f'Number of functions to process per Claude command (default: {FUNCTIONS_PER_COMMAND})')
    parser.add_argument('--timeout', type=int, default=300,
                       help='Timeout in seconds for each function batch (default: 300)')
    
    args = parser.parse_args()
    
    orchestrator = FunctionSimplificationOrchestrator(
        function_list_file=args.input,
        max_parallel=args.parallel,
        functions_per_command=args.batch_size,
        timeout_seconds=args.timeout
    )
    
    orchestrator.run()


if __name__ == "__main__":
    main()
