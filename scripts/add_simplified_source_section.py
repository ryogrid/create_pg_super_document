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
    
    def _build_prompt(self, function_name: str) -> str:
        """
        Build the combined prompt for Claude that includes both main task and subagent logic.
        """
        prompt = f"""# Function Source Code Simplification Task

You are processing the PostgreSQL function: **{function_name}**

## Your Task
1. Retrieve the source code for this function
2. Create a simplified, readable version that preserves essential logic
3. Append the simplified version to the existing documentation
4. Report completion

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

### Step 3: Update Documentation File
1. Check if documentation exists at: `generated_docs/{function_name[0]}/{function_name}.md`
2. Read existing content to avoid duplication
3. Check if "## Simplified Source" section already exists - if so, skip this function
4. Append new section "## Simplified Source" with the simplified code
5. Format with proper C syntax highlighting

## Output Format

Append this section to the documentation:

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
- If the symbol is not a function, skip it and report appropriately
- If MCP tools are not accessible, report the issue and skip

## Completion
When done, create a file named `output/temp/{function_name}_simplification_complete.txt` with content "finished" to signal completion.

## Error Handling
- If pg_symbol_source returns an error or empty result, try pg_symbol_document as fallback
- If the symbol is not found in any tool, create completion file with "skipped: not found"
- If the documentation file doesn't exist, create it with minimal header before appending

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
                
                # Update status
                with self.lock:
                    self.in_progress[worker_id] = function_name
                    self.stats['in_progress'] = len(self.in_progress)
                
                logging.info(f"[Worker {worker_id}] Starting: {function_name}")
                
                # Build prompt
                prompt = self._build_prompt(function_name)
                
                # Clean up any previous completion marker
                completion_file = Path(f'output/temp/{function_name}_simplification_complete.txt')
                if completion_file.exists():
                    completion_file.unlink()
                
                # Run Claude with MCP server access
                try:
                    result = subprocess.run(
                        [
                            'claude',
                            '--allowedTools', 'mcp,Read,Write',  # Enable MCP server access
                            '-p', prompt,
                            '--model', 'claude-sonnet-4-20250514',
                            '--max-turns', '10',
                            '--permission-mode', 'bypassPermissions'
                        ],
                        capture_output=True,
                        text=True,
                        timeout=self.timeout_seconds,
                        cwd=str(Path.cwd())
                    )
                    
                    # Check for completion
                    if completion_file.exists():
                        completion_content = completion_file.read_text().strip()
                        if "skipped" in completion_content:
                            with self.lock:
                                self.failed.append((function_name, completion_content))
                                self.stats['failed'] += 1
                            logging.warning(f"[Worker {worker_id}] ⊘ Skipped: {function_name} ({completion_content})")
                        else:
                            with self.lock:
                                self.completed.append(function_name)
                                self.stats['completed'] += 1
                            logging.info(f"[Worker {worker_id}] ✓ Completed: {function_name}")
                    else:
                        with self.lock:
                            self.failed.append((function_name, "No completion marker"))
                            self.stats['failed'] += 1
                        logging.warning(f"[Worker {worker_id}] ✗ Failed (no completion): {function_name}")
                    
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
                    if (self.stats['completed'] + self.stats['failed']) % 5 == 0:
                        self._report_progress()
                    
                    # Detailed summary every 20 functions
                    if (self.stats['completed'] + self.stats['failed']) % 20 == 0:
                        self._report_summary()
                    
                    # Rate limiting
                    time.sleep(2)
                    
            except Exception as e:
                logging.error(f"[Worker {worker_id}] Unexpected error: {e}")
                time.sleep(5)
    
    def _report_progress(self):
        """Report current progress."""
        with self.lock:
            processed = self.stats['completed'] + self.stats['failed']
            percentage = (processed / self.stats['total'] * 100) if self.stats['total'] > 0 else 0
            
            logging.info(f"\n{'='*60}")
            logging.info(f"[Progress Update - {processed}/{self.stats['total']} ({percentage:.1f}%)]")
            logging.info(f"Completed: {self.stats['completed']}, Failed: {self.stats['failed']}, In Progress: {self.stats['in_progress']}")
            
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
            logging.info(f"Total Progress: {self.stats['completed'] + self.stats['failed']}/{self.stats['total']}")
            logging.info(f"Success Rate: {self.stats['completed']}/{self.stats['completed'] + self.stats['failed']}")
            logging.info(f"Elapsed Time: {elapsed_str}")
            
            if elapsed > 0:
                rate = (self.stats['completed'] + self.stats['failed']) / (elapsed / 60)
                logging.info(f"Processing Rate: {rate:.1f} functions/minute")
                
                remaining = self.stats['total'] - (self.stats['completed'] + self.stats['failed'])
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
        
        # Ensure output directory exists
        Path('output/temp').mkdir(parents=True, exist_ok=True)
        
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
        logging.info(f"Total functions processed: {self.stats['completed'] + self.stats['failed']}/{self.stats['total']}")
        logging.info(f"Successfully simplified: {self.stats['completed']}")
        logging.info(f"Failed: {self.stats['failed']}")
        logging.info(f"Total time: {elapsed_str}")
        
        if self.stats['completed'] + self.stats['failed'] > 0:
            success_rate = (self.stats['completed'] / (self.stats['completed'] + self.stats['failed'])) * 100
            logging.info(f"Success rate: {success_rate:.1f}%")
        
        if self.failed:
            logging.info("\nFailed functions:")
            for func, reason in self.failed:
                logging.info(f"  - {func}: {reason}")
        
        # Save results to file
        results = {
            'completed': self.completed,
            'failed': [{'function': f, 'reason': r} for f, r in self.failed],
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
