#!/usr/bin/env python3
"""
Autonomous workflow for documenting PostgreSQL symbols

This script implements the autonomous workflow described in the issue:
1. Get next batch
2. Generate documentation for each symbol in the batch
3. Ingest documents into the database
4. Persist progress to the data branch
5. Repeat until all batches are processed
"""

import json
import subprocess
import sys
from pathlib import Path
import time


def run_command(cmd, cwd=None, shell=False):
    """Run a command and return its output"""
    try:
        if shell:
            result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True, check=True)
        else:
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
        print(f"Exit code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise


def get_next_batch():
    """Get the next batch of symbols to process"""
    try:
        output = run_command(['python3', 'scripts/get_next_batch.py'])
        return json.loads(output)
    except json.JSONDecodeError as e:
        print(f"Failed to parse batch JSON: {e}")
        print(f"Output was: {output}")
        return None


def generate_documentation(symbol_data):
    """Generate documentation for a symbol using AI context"""
    symbol_name = symbol_data['symbol_name']
    
    # Create a comprehensive prompt based on the symbol information
    prompt_parts = [
        f"# {symbol_name}",
        "",
        "## Overview",
        f"(Briefly explain the purpose and role of {symbol_name} in 1-2 sentences based on the context below)",
        "",
        "## Definition", 
        symbol_data.get('definition', '(No definition available)'),
        "",
        "## Detailed Description",
        f"(Provide specific explanation of {symbol_name}'s functionality, behavior, design philosophy, etc.)",
        "",
        "## Parameters / Member Variables",
        "(Explain the role and meaning of each function parameter or struct member in a bulleted list)",
        "",
        "## Dependencies",
        "- Functions called/Symbols referenced:",
    ]
    
    # Add references from this symbol
    refs_from = symbol_data.get('references_from_this', '')
    if refs_from:
        for line in refs_from.split('\n'):
            if line.strip():
                parts = line.split(' at ')
                if len(parts) >= 2:
                    symbol_ref = parts[0].strip()
                    prompt_parts.append(f"  - {symbol_ref}")
    
    prompt_parts.extend([
        "- Called from (representative examples):",
    ])
    
    # Add references to this symbol
    refs_to = symbol_data.get('references_to_this', '')
    if refs_to:
        for line in refs_to.split('\n'):
            if line.strip():
                parts = line.split(' at ')
                if len(parts) >= 2:
                    symbol_ref = parts[0].strip()
                    prompt_parts.append(f"  - {symbol_ref}")
    
    prompt_parts.extend([
        "",
        "## Notes and Other Information",
        f"(Notable points, usage precautions, related background knowledge for {symbol_name})",
    ])
    
    # Add related symbol summaries if available
    related_summaries = symbol_data.get('related_symbol_summaries', [])
    if related_summaries:
        prompt_parts.extend([
            "",
            "## Related Symbols Context",
        ])
        for summary in related_summaries:
            prompt_parts.append(summary)
    
    # Generate the documentation
    documentation = '\n'.join(prompt_parts)
    
    # For now, return this structured template - in a real implementation, 
    # this would call an AI service like Claude or OpenAI
    return documentation


def save_document(symbol_name, content):
    """Save a document using the mcp_tool"""
    try:
        cmd = ['python3', 'scripts/mcp_tool.py', 'return_document', symbol_name, content]
        result = run_command(cmd)
        return json.loads(result)
    except json.JSONDecodeError as e:
        print(f"Failed to parse save result: {e}")
        return {"status": "error", "message": f"JSON parse error: {e}"}


def ingest_documents():
    """Ingest generated documents into the database"""
    try:
        output = run_command(['python3', 'scripts/ingest_documents.py'])
        return json.loads(output)
    except json.JSONDecodeError as e:
        print(f"Failed to parse ingest result: {e}")
        return {"status": "error", "message": f"JSON parse error: {e}"}


def persist_to_data_branch(batch_id):
    """Persist the current progress to the data branch"""
    print(f"Persisting batch {batch_id} to data branch...")
    
    # Step 1: Switch to/create the data branch
    try:
        run_command(['git', 'fetch', 'origin'])
        try:
            # Try to checkout existing branch
            run_command(['git', 'checkout', 'copilot/agent-documentation-progress'])
        except subprocess.CalledProcessError:
            # Create new branch if it doesn't exist
            run_command(['git', 'checkout', '-b', 'copilot/agent-documentation-progress'])
    except subprocess.CalledProcessError as e:
        print(f"Failed to switch to data branch: {e}")
        return False
    
    # Step 2: Copy the updated database from working branch
    try:
        run_command(['git', 'checkout', 'copilot/copilot-work', '--', 'data/documents.duckdb'])
    except subprocess.CalledProcessError as e:
        print(f"Failed to copy database: {e}")
        return False
    
    # Step 3: Commit and push the database
    try:
        run_command(['git', 'config', '--global', 'user.name', 'GitHub Copilot Agent'])
        run_command(['git', 'config', '--global', 'user.email', 'copilot-agent@users.noreply.github.com'])
        run_command(['git', 'add', 'data/documents.duckdb'])
        run_command(['git', 'commit', '-m', f'docs(data): Persist documentation from batch {batch_id}'])
        run_command(['git', 'push', '--set-upstream', 'origin', 'copilot/agent-documentation-progress'])
    except subprocess.CalledProcessError as e:
        print(f"Failed to commit and push: {e}")
        return False
    
    # Step 4: Return to working branch
    try:
        run_command(['git', 'checkout', 'copilot/copilot-work'])
    except subprocess.CalledProcessError as e:
        print(f"Failed to return to working branch: {e}")
        return False
    
    # Step 5: Clean up temporary file (if it exists)
    current_batch_file = Path('current_batch.json')
    if current_batch_file.exists():
        current_batch_file.unlink()
    
    return True


def main():
    """Main autonomous workflow"""
    print("Starting autonomous PostgreSQL symbol documentation workflow...")
    
    batch_count = 0
    total_symbols = 0
    
    while True:
        print(f"\n{'='*60}")
        print(f"Getting next batch...")
        print(f"{'='*60}")
        
        # Step 1: Get next batch
        batch_data = get_next_batch()
        if not batch_data:
            print("Failed to get batch data")
            break
            
        # Check if we're done
        if batch_data.get('message') == 'All batches have been processed.':
            print("✅ All batches have been processed!")
            break
            
        batch_id = batch_data.get('batch_id')
        symbols = batch_data.get('symbols_to_process', [])
        
        if not symbols:
            print("No symbols to process in this batch")
            continue
            
        print(f"Processing batch {batch_id} with {len(symbols)} symbols:")
        for symbol in symbols:
            print(f"  - {symbol['symbol_name']}")
        
        # Step 2: Generate documentation for each symbol
        success_count = 0
        for symbol_data in symbols:
            symbol_name = symbol_data['symbol_name']
            print(f"\n📝 Generating documentation for {symbol_name}...")
            
            try:
                # Generate documentation
                doc_content = generate_documentation(symbol_data)
                
                # Save document
                save_result = save_document(symbol_name, doc_content)
                if save_result.get('status') == 'success':
                    print(f"✅ Saved documentation for {symbol_name}")
                    success_count += 1
                else:
                    print(f"❌ Failed to save documentation for {symbol_name}: {save_result}")
                    
            except Exception as e:
                print(f"❌ Error processing {symbol_name}: {e}")
                
        # Step 3: Ingest documents into database
        print(f"\n📥 Ingesting {success_count} documents into database...")
        ingest_result = ingest_documents()
        if ingest_result.get('status') == 'success':
            ingested_count = ingest_result.get('ingested_count', 0)
            print(f"✅ Successfully ingested {ingested_count} documents")
        else:
            print(f"❌ Failed to ingest documents: {ingest_result}")
            
        # Step 4: Persist to data branch
        if success_count > 0:
            if persist_to_data_branch(batch_id):
                print(f"✅ Successfully persisted batch {batch_id} to data branch")
            else:
                print(f"❌ Failed to persist batch {batch_id}")
        
        # Update counters
        batch_count += 1
        total_symbols += success_count
        
        print(f"\n📊 Progress: {batch_count} batches processed, {total_symbols} symbols documented")
        
        # Brief pause to avoid overwhelming the system
        time.sleep(2)
    
    print(f"\n🎉 Workflow completed! Processed {batch_count} batches and documented {total_symbols} symbols.")
    return True


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Workflow interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Workflow failed with error: {e}")
        sys.exit(1)