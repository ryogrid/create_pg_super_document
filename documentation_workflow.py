#!/usr/bin/env python3
"""
PostgreSQL Documentation Agent - Complete Autonomous Workflow
This script implements the full autonomous documentation workflow for PostgreSQL symbols.
"""

import json
import sys
from pathlib import Path
import subprocess

# Add scripts directory to path
sys.path.append(str(Path(__file__).parent / "scripts"))

try:
    from mcp_tool import return_document
except ImportError:
    print("[ERROR] Could not import mcp_tool functions")
    sys.exit(1)

# Import the documentation generation functions
from autonomous_documentation_agent import (
    generate_documentation_for_symbol,
    run_command
)


def process_multiple_batches(num_batches=5):
    """Process multiple batches of PostgreSQL symbols"""
    print(f"[INFO] Starting documentation of {num_batches} batches of PostgreSQL symbols")
    
    total_processed = 0
    completed_batches = []
    
    for batch_num in range(1, num_batches + 1):
        print(f"\n[INFO] === Processing Batch {batch_num}/{num_batches} ===")
        
        try:
            # Get next batch
            success, output = run_command("python3 scripts/get_next_batch.py > current_batch.json", 
                                        "Getting next batch")
            if not success:
                print(f"[ERROR] Failed to get batch: {output}")
                break
            
            # Load batch data
            try:
                with open('current_batch.json', 'r') as f:
                    batch_data = json.load(f)
            except Exception as e:
                print(f"[ERROR] Failed to read batch data: {e}")
                break
            
            if not batch_data.get('symbols_to_process'):
                print("[INFO] No more symbols to process!")
                break
            
            batch_id = batch_data.get('batch_id', 'unknown')
            symbols = batch_data['symbols_to_process']
            
            print(f"[INFO] Processing batch {batch_id} with {len(symbols)} symbols")
            
            # Generate documentation for each symbol
            processed_symbols = []
            for symbol_data in symbols:
                symbol_name = symbol_data["symbol_name"]
                try:
                    print(f"[INFO] Generating documentation for: {symbol_name}")
                    doc_content = generate_documentation_for_symbol(symbol_data)
                    
                    # Save document
                    result = return_document(symbol_name, doc_content)
                    if result.get("status") == "success":
                        processed_symbols.append(symbol_name)
                        print(f"[INFO] ✓ {symbol_name}")
                    else:
                        print(f"[WARNING] Failed to save {symbol_name}: {result}")
                        
                except Exception as e:
                    print(f"[WARNING] Error processing {symbol_name}: {e}")
                    continue
            
            # Ingest the documents
            if processed_symbols:
                success, output = run_command("python3 scripts/ingest_documents.py", 
                                            "Ingesting documents")
                if success:
                    total_processed += len(processed_symbols)
                    completed_batches.append({
                        'batch_id': batch_id,
                        'symbols_count': len(processed_symbols),
                        'symbols': processed_symbols
                    })
                    print(f"[INFO] ✓ Batch {batch_id} completed: {len(processed_symbols)} symbols")
                else:
                    print(f"[ERROR] Failed to ingest documents: {output}")
            
            # Clean up
            run_command("rm -f current_batch.json", "Cleaning up")
            
        except KeyboardInterrupt:
            print("\n[INFO] Interrupted by user")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error in batch {batch_num}: {e}")
            continue
    
    return {
        'total_processed': total_processed,
        'completed_batches': completed_batches
    }


def main():
    """Main entry point for the documentation workflow"""
    print("[INFO] PostgreSQL Autonomous Documentation Agent Starting...")
    
    # Ensure we're in the correct directory
    if not Path("scripts/get_next_batch.py").exists():
        print("[ERROR] This script must be run from the repository root")
        sys.exit(1)
    
    # Process multiple batches
    results = process_multiple_batches(num_batches=10)
    
    print(f"\n[INFO] === Documentation Session Complete ===")
    print(f"[INFO] Total symbols processed: {results['total_processed']}")
    print(f"[INFO] Batches completed: {len(results['completed_batches'])}")
    
    if results['completed_batches']:
        print(f"[INFO] Batch details:")
        for batch in results['completed_batches']:
            print(f"  - Batch {batch['batch_id']}: {batch['symbols_count']} symbols")
    
    return results


if __name__ == "__main__":
    main()