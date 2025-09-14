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


def process_multiple_batches(max_batches=None):
    """Process multiple batches of PostgreSQL symbols - all remaining if max_batches is None"""
    if max_batches is None:
        print("[INFO] Starting documentation of ALL remaining PostgreSQL symbols")
    else:
        print(f"[INFO] Starting documentation of up to {max_batches} batches of PostgreSQL symbols")
    
    total_processed = 0
    completed_batches = []
    batch_num = 0
    errors_count = 0
    max_consecutive_errors = 5
    consecutive_errors = 0
    
    while True:
        batch_num += 1
        
        # Check if we should stop due to max_batches limit
        if max_batches is not None and batch_num > max_batches:
            print(f"[INFO] Reached maximum batch limit ({max_batches}). Stopping.")
            break
        
        # Check if we've had too many consecutive errors
        if consecutive_errors >= max_consecutive_errors:
            print(f"[ERROR] Too many consecutive errors ({consecutive_errors}). Stopping for safety.")
            break
        
        print(f"\n[INFO] === Processing Batch {batch_num} ===")
        
        try:
            # Get next batch
            success, output = run_command("python3 scripts/get_next_batch.py > current_batch.json", 
                                        "Getting next batch")
            if not success:
                print(f"[ERROR] Failed to get batch: {output}")
                consecutive_errors += 1
                errors_count += 1
                continue
            
            # Load batch data
            try:
                with open('current_batch.json', 'r') as f:
                    batch_data = json.load(f)
            except Exception as e:
                print(f"[ERROR] Failed to read batch data: {e}")
                consecutive_errors += 1
                errors_count += 1
                continue
            
            # Check if all processing is complete
            if not batch_data.get('symbols_to_process') or batch_data.get('message') == "All batches have been processed.":
                print("[INFO] All symbols have been processed! Documentation complete.")
                break
            
            batch_id = batch_data.get('batch_id', 'unknown')
            symbols = batch_data['symbols_to_process']
            
            print(f"[INFO] Processing batch {batch_id} with {len(symbols)} symbols")
            
            # Generate documentation for each symbol
            processed_symbols = []
            symbol_errors = 0
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
                        symbol_errors += 1
                        
                except Exception as e:
                    print(f"[WARNING] Error processing {symbol_name}: {e}")
                    symbol_errors += 1
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
                        'symbols': processed_symbols,
                        'symbol_errors': symbol_errors
                    })
                    print(f"[INFO] ✓ Batch {batch_id} completed: {len(processed_symbols)} symbols processed, {symbol_errors} errors")
                    consecutive_errors = 0  # Reset consecutive error counter on success
                else:
                    print(f"[ERROR] Failed to ingest documents: {output}")
                    consecutive_errors += 1
                    errors_count += 1
            else:
                print(f"[WARNING] No symbols processed in batch {batch_id}")
                consecutive_errors += 1
                errors_count += 1
            
            # Clean up
            run_command("rm -f current_batch.json", "Cleaning up")
            
            # Progress report every 10 batches
            if batch_num % 10 == 0:
                print(f"\n[PROGRESS] Completed {batch_num} batches, {total_processed} symbols total")
                print(f"[PROGRESS] Error rate: {errors_count}/{batch_num} = {errors_count/batch_num*100:.1f}%")
            
        except KeyboardInterrupt:
            print("\n[INFO] Interrupted by user")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error in batch {batch_num}: {e}")
            consecutive_errors += 1
            errors_count += 1
            continue
    
    return {
        'total_processed': total_processed,
        'completed_batches': completed_batches,
        'total_batches_attempted': batch_num,
        'total_errors': errors_count
    }


def main():
    """Main entry point for the documentation workflow"""
    print("[INFO] PostgreSQL Autonomous Documentation Agent Starting...")
    
    # Ensure we're in the correct directory
    if not Path("scripts/get_next_batch.py").exists():
        print("[ERROR] This script must be run from the repository root")
        sys.exit(1)
    
    # Process ALL remaining batches (no limit)
    print("[INFO] Processing ALL remaining batches...")
    results = process_multiple_batches(max_batches=None)
    
    print(f"\n[INFO] === Documentation Session Complete ===")
    print(f"[INFO] Total symbols processed: {results['total_processed']}")
    print(f"[INFO] Batches completed: {len(results['completed_batches'])}")
    print(f"[INFO] Total batches attempted: {results['total_batches_attempted']}")
    print(f"[INFO] Total errors encountered: {results['total_errors']}")
    
    if results['completed_batches']:
        print(f"[INFO] Sample batch details (last 5):")
        for batch in results['completed_batches'][-5:]:
            print(f"  - Batch {batch['batch_id']}: {batch['symbols_count']} symbols, {batch.get('symbol_errors', 0)} errors")
    
    # Final statistics
    if results['total_processed'] > 0:
        avg_symbols_per_batch = results['total_processed'] / len(results['completed_batches']) if results['completed_batches'] else 0
        print(f"[INFO] Average symbols per batch: {avg_symbols_per_batch:.1f}")
        
    return results


if __name__ == "__main__":
    main()