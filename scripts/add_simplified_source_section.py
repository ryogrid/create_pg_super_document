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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simplification_progress.log'),
        logging.StreamHandler()
    ]
)

class FunctionSimplificationOrchestrator:
    def __init__(self, 
                 function_list_file: str = 'experimental/function_call_hierarchy.txt',
                 max_parallel: int = 3,
                 timeout_seconds: int = 300):
        """
        Initialize the orchestrator.
        
        Args:
            function_list_file: Path to file containing function names (one per line)
            max_parallel: Number of parallel Claude processes to run
            timeout_seconds: Timeout for each Claude invocation
        """
        self.function_list_file = Path(function_list_file)
        self.max_parallel = max_parallel
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
        for func in self.functions:
            self.work_queue.put(func)
        
        # Statistics
        self.start_time = None
        self.stats = {
            'total': len(self.functions),
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'in_progress': 0
        }
    
    def _load_function_list(self) -> List[str]:
        """Load function names from file."""
        if not self.function_list_file.exists():
            logging.error(f"Function list file not found: {self.function_list_file}")
            return []
        
        with open(self.function_list_file, 'r') as f:
            functions = [line.strip() for line in f if line.strip()]
        
        logging.info(f"Loaded {len(functions)} functions from {self.function_list_file}")
        return functions
    
    def _check_simplified_source_exists(self, function_name: str) -> bool:
        """
        Check if simplified source section already exists in the markdown file.
        """
        first_letter = function_name[0].upper()
        doc_path = Path(f'generated_docs/{first_letter}/{function_name}.md')
        
        if not doc_path.exists():
            return False
        
        try:
            content = doc_path.read_text(encoding='utf-8')
            return '## Simplified Source' in content
        except Exception:
            return False
    
    def _build_prompt(self, function_name: str) -> str:
        """
        Build the combined prompt for Claude that includes both main task and subagent logic.
        """
        first_letter = function_name[0].upper()
        doc_path = f'generated_docs/{first_letter}/{function_name}.md'
        
        prompt = f"""# Function Source Code Simplification Task

You are processing the PostgreSQL function: **{function_name}**

## Your Task
1. Retrieve the source code for this function
2. Create a simplified, readable version that preserves essential logic
3. Directly append the simplified version to the existing documentation file
4. Complete the task without creating any temporary files

## Available MCP Server Tools

You have access to the following MCP server functions for PostgreSQL symbol information:

- **pg_symbol_source(symbol_name)**: Retrieve the complete source code for a symbol
- **pg_symbol_overview(symbol_name)**: Get a concise overview/summary of the symbol
- **pg_symbol_document(symbol_name)**: Fetch detailed documentation for the symbol
- **pg_references_from(symbol_name)**: List symbols referenced by the given symbol
- **pg_references_to(symbol_name)**: List symbols that reference the given symbol

These are available through the MCP server configured in your environment.

## Processing Steps

### Step 1: Retrieve Source Code
- Use MCP tool: `pg_symbol_source("{function_name}")`
- Verify this is a function (not struct/typedef/etc.)
- The tool will return the complete source code from the PostgreSQL codebase

### Step 2: Simplify the Code
Apply these simplification techniques:
- Remove non-essential error handling (keep only critical checks)
- Simplify complex conditions into clearer logic flow
- Replace detailed memory operations with high-level comments
- Use descriptive variable names instead of cryptic ones
- Add brief explanatory comments for complex logic
- Remove platform-specific code, focus on main logic path
- Consolidate similar cases or branches

Target: 20-50% of original length while preserving essential algorithm

### Step 3: Update Documentation File Directly
1. Check if documentation exists at: `{doc_path}`
2. If file doesn't exist, create it with a basic header
3. Read the existing content
4. Check if "## Simplified Source" section already exists - if so, print "ALREADY_PROCESSED: {function_name}" and stop
5. Append the new section "## Simplified Source" with the simplified code directly to the file
6. Use the Write tool to update the file

## Output Format

Append this section directly to `{doc_path}`:

```markdown
## Simplified Source

```c
// Simplified version of {function_name}
ReturnType {function_name}(Parameters) {{
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

Key simplifications made:
- [List major simplifications made]
- [e.g., "Removed detailed error handling for clarity"]
- [e.g., "Consolidated multiple similar branches"]
- [e.g., "Abstracted low-level memory operations"]
```

## Important Guidelines
- Preserve the essential algorithm and logic flow
- Don't oversimplify to lose important functionality  
- Maintain correctness - represent what the function actually does
- Use consistent formatting throughout
- If the symbol is not a function, print "NOT_A_FUNCTION: {function_name}" and stop
- If MCP tools are not accessible, print "MCP_ERROR: {function_name}" and stop
- **DO NOT create any temporary files - directly update the markdown file**

## Completion Confirmation
When successfully completed, print: "COMPLETED: {function_name}"
This confirms the simplified source has been added to the documentation.

## Error Handling
- If pg_symbol_source returns an error or empty result, try pg_symbol_document as fallback
- If the symbol is not found in any tool, print "NOT_FOUND: {function_name}" and stop
- If any error occurs during file writing, print "WRITE_ERROR: {function_name}" and stop

Start processing now for function: {function_name}"""
        
        return prompt
    
    def _process_function(self, worker_id: int):
        """
        Worker thread that processes functions from the queue.
        """
        while True:
            try:
                # Get next function from queue (with timeout to check for completion)
                try:
                    function_name = self.work_queue.get(timeout=1)
                except queue.Empty:
                    # Check if we're done
                    if self.work_queue.empty() and len(self.in_progress) == 0:
                        break
                    continue
                
                # Check if already processed
                if self._check_simplified_source_exists(function_name):
                    with self.lock:
                        self.skipped.append(function_name)
                        self.stats['skipped'] += 1
                    logging.info(f"[Worker {worker_id}] ⊘ Already processed: {function_name}")
                    self.work_queue.task_done()
                    continue
                
                # Update status
                with self.lock:
                    self.in_progress[worker_id] = function_name
                    self.stats['in_progress'] = len(self.in_progress)
                
                logging.info(f"[Worker {worker_id}] Starting: {function_name}")
                
                # Build prompt
                prompt = self._build_prompt(function_name)
                
                # Run Claude with MCP server access
                try:
                    result = subprocess.run(
                        [
                            'claude',
                            '--allowedTools', 'mcp,Read,Write',  # Enable MCP server access and file operations
                            '-p', prompt,
                            '--model', 'claude-sonnet-4-20250514',
                            '--permission-mode', 'bypassPermissions',
                            '--max-turns', str(80),
                        ],
                        capture_output=True,
                        text=True,
                        timeout=self.timeout_seconds,
                        cwd=str(Path.cwd())
                    )
                    
                    # Check output for status messages
                    output = result.stdout + result.stderr
                    
                    if "COMPLETED:" in output:
                        # Verify the file was actually updated
                        if self._check_simplified_source_exists(function_name):
                            with self.lock:
                                self.completed.append(function_name)
                                self.stats['completed'] += 1
                            logging.info(f"[Worker {worker_id}] ✓ Completed: {function_name}")
                        else:
                            with self.lock:
                                self.failed.append((function_name, "File not updated"))
                                self.stats['failed'] += 1
                            logging.warning(f"[Worker {worker_id}] ✗ Failed (file not updated): {function_name}")
                    
                    elif "ALREADY_PROCESSED:" in output:
                        with self.lock:
                            self.skipped.append(function_name)
                            self.stats['skipped'] += 1
                        logging.info(f"[Worker {worker_id}] ⊘ Already had simplified source: {function_name}")
                    
                    elif "NOT_A_FUNCTION:" in output:
                        with self.lock:
                            self.skipped.append(function_name)
                            self.stats['skipped'] += 1
                        logging.info(f"[Worker {worker_id}] ⊘ Not a function: {function_name}")
                    
                    elif "NOT_FOUND:" in output:
                        with self.lock:
                            self.failed.append((function_name, "Symbol not found"))
                            self.stats['failed'] += 1
                        logging.warning(f"[Worker {worker_id}] ✗ Symbol not found: {function_name}")
                    
                    elif "MCP_ERROR:" in output or "WRITE_ERROR:" in output:
                        error_type = "MCP error" if "MCP_ERROR:" in output else "Write error"
                        with self.lock:
                            self.failed.append((function_name, error_type))
                            self.stats['failed'] += 1
                        logging.error(f"[Worker {worker_id}] ✗ {error_type}: {function_name}")
                    
                    elif "Session limit reached" in output or "Rate limit exceeded" in output:
                        with self.lock:
                            self.failed.append((function_name, "Session/Rate limit"))
                            self.stats['failed'] += 1
                            logging.warning(f"[Worker {worker_id}] ✗ Failed ({output}): {function_name}")
                        time.sleep(3600)  # Sleep for an hour before retrying
                    else:
                        with self.lock:
                                self.failed.append((function_name, "Unknown error"))
                                self.stats['failed'] += 1
                                logging.warning(f"[Worker {worker_id}] ✗ Failed (unknown): {function_name} {output[:100]}...")
                    
                except subprocess.TimeoutExpired:
                    with self.lock:
                        self.failed.append((function_name, "Timeout"))
                        self.stats['failed'] += 1
                    logging.error(f"[Worker {worker_id}] ✗ Timeout: {function_name}")
                
                except Exception as e:
                    with self.lock:
                        self.failed.append((function_name, str(e)))
                        self.stats['failed'] += 1
                    logging.error(f"[Worker {worker_id}] ✗ Error processing {function_name}: {e}")
                
                finally:
                    # Remove from in-progress
                    with self.lock:
                        if worker_id in self.in_progress:
                            del self.in_progress[worker_id]
                        self.stats['in_progress'] = len(self.in_progress)
                    
                    # Mark queue task as done
                    self.work_queue.task_done()
                    
                    # Report progress periodically
                    total_processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
                    if total_processed % 5 == 0:
                        self._report_progress()
                    
                    # Detailed summary every 20 functions
                    if total_processed % 20 == 0:
                        self._report_summary()
                    
                    # Rate limiting
                    time.sleep(2)
                    
            except Exception as e:
                logging.error(f"[Worker {worker_id}] Unexpected error: {e}")
                time.sleep(5)
    
    def _report_progress(self):
        """Report current progress."""
        with self.lock:
            processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
            percentage = (processed / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
            
            logging.info(f"\n{'='*60}")
            logging.info(f"[Progress Update - {processed}/{self.stats['total']} ({percentage:.1f}%)]")
            logging.info(f"Completed: {self.stats['completed']}, Failed: {self.stats['failed']}, Skipped: {self.stats['skipped']}, In Progress: {self.stats['in_progress']}")
            
            # Show recently completed
            if self.completed:
                recent = self.completed[-5:]
                logging.info("Recently completed:")
                for func in recent:
                    logging.info(f"  ✓ {func}")
            
            # Show recent failures
            if self.failed:
                recent_failed = self.failed[-3:]
                logging.info("Recent failures:")
                for func, reason in recent_failed:
                    logging.info(f"  ✗ {func}: {reason}")
            
            logging.info(f"{'='*60}\n")
    
    def _report_summary(self):
        """Report detailed summary."""
        with self.lock:
            elapsed = time.time() - self.start_time if self.start_time else 0
            elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
            
            logging.info(f"\n{'='*70}")
            logging.info(f"[SUMMARY REPORT]")
            logging.info(f"{'='*70}")
            processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
            logging.info(f"Total Progress: {processed}/{self.stats['total']}")
            logging.info(f"Success Rate: {self.stats['completed']}/{processed - self.stats['skipped']} (excluding skipped)")
            logging.info(f"Elapsed Time: {elapsed_str}")
            
            if elapsed > 0:
                rate = processed / (elapsed / 60)
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
        logging.info(f"Timeout per function: {self.timeout_seconds}s")
        logging.info(f"{'='*70}\n")
        
        # Start worker threads
        workers = []
        for i in range(self.max_parallel):
            worker = threading.Thread(target=self._process_function, args=(i,))
            worker.start()
            workers.append(worker)
        
        # Wait for all workers to complete
        for worker in workers:
            worker.join()
        
        # Final report
        self._final_report()
    
    def _final_report(self):
        """Generate final completion report."""
        elapsed = time.time() - self.start_time
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        
        logging.info(f"\n{'='*70}")
        logging.info(f"[FINAL REPORT]")
        logging.info(f"{'='*70}")
        total_processed = self.stats['completed'] + self.stats['failed'] + self.stats['skipped']
        logging.info(f"Total functions processed: {total_processed}/{self.stats['total']}")
        logging.info(f"Successfully simplified: {self.stats['completed']}")
        logging.info(f"Failed: {self.stats['failed']}")
        logging.info(f"Skipped (already processed or not functions): {self.stats['skipped']}")
        logging.info(f"Total time: {elapsed_str}")
        
        if total_processed - self.stats['skipped'] > 0:
            success_rate = (self.stats['completed'] / (total_processed - self.stats['skipped'])) * 100
            logging.info(f"Success rate (excluding skipped): {success_rate:.1f}%")
        
        if self.failed:
            logging.info("\nFailed functions:")
            for func, reason in self.failed:
                logging.info(f"  - {func}: {reason}")
        
        # Save results to file
        results = {
            'completed': self.completed,
            'failed': [{'function': f, 'reason': r} for f, r in self.failed],
            'skipped': self.skipped,
            'stats': self.stats,
            'elapsed_seconds': elapsed
        }
        
        with open('simplification_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logging.info(f"\nResults saved to simplification_results.json")
        logging.info(f"{'='*70}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Simplify PostgreSQL function source code in parallel using MCP server')
    parser.add_argument('--input', default='experimental/function_call_hierarchy.txt',
                       help='Input file with function names (one per line)')
    parser.add_argument('--parallel', type=int, default=3,
                       help='Number of parallel Claude processes')
    parser.add_argument('--timeout', type=int, default=300,
                       help='Timeout in seconds for each function')
    
    args = parser.parse_args()
    
    orchestrator = FunctionSimplificationOrchestrator(
        function_list_file=args.input,
        max_parallel=args.parallel,
        timeout_seconds=args.timeout
    )
    
    orchestrator.run()


if __name__ == "__main__":
    main()
