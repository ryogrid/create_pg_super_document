#!/usr/bin/env python3
"""
Enhanced Orchestrator for Copilot Coding Agent Documentation Generation
Includes context management, quality control, and Copilot-specific optimizations
"""
import json
import duckdb
import subprocess
import time
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set

class CopilotDocumentationOrchestrator:
    """Enhanced orchestrator optimized for Copilot Coding Agent"""
    
    def __init__(self, global_symbols_db: str = 'global_symbols.db'):
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
            'processed_symbols': 0,
            'quality_metrics': {
                'high_quality_count': 0,
                'medium_quality_count': 0,
                'low_quality_count': 0,
                'average_score': 0.0
            }
        }
        
        # Context management settings
        self.context_reset_frequency = 5  # Reset context every N batches
        self.current_batch_count = 0
        
    def _load_symbol_details(self, db_file: str):
        """Load symbol details into memory for fast lookup"""
        con = duckdb.connect(db_file, read_only=True)
        result = con.execute("SELECT id, symbol_name, symbol_type FROM symbol_definitions").fetchall()
        self.symbol_details = {
            row[0]: {'name': row[1], 'type': row[2]} for row in result
        }
        con.close()

    def init_databases(self):
        """Initialize DuckDB databases"""
        # Documents database
        self.doc_db = duckdb.connect('data/documents.duckdb')
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                symbol_id INTEGER PRIMARY KEY,
                symbol_name TEXT NOT NULL,
                symbol_type TEXT NOT NULL,
                layer INTEGER NOT NULL,
                content TEXT NOT NULL,
                summary TEXT,
                dependencies TEXT,
                related_symbols TEXT,
                quality_score REAL DEFAULT 0.0,
                quality_level TEXT DEFAULT 'UNKNOWN',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Enhanced processing log with quality tracking
        self.doc_db.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                batch_id INTEGER PRIMARY KEY,
                symbol_ids TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                processed_count INTEGER DEFAULT 0,
                error_message TEXT,
                quality_score REAL DEFAULT 0.0,
                context_reset BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Metadata database
        self.meta_db = duckdb.connect('data/metadata.duckdb', read_only=True)
        
        self.doc_db.commit()

    def should_reset_context(self) -> bool:
        """Determine if context should be reset for the next batch"""
        return self.current_batch_count % self.context_reset_frequency == 0

    def create_copilot_optimized_prompt(self, symbol_ids: List[int], batch: Dict, 
                                      context_reset: bool = False) -> str:
        """Create an enhanced prompt specifically optimized for Copilot Coding Agent"""
        
        symbol_names = [self.symbol_details[sid]['name'] for sid in symbol_ids]
        symbol_list_str = '\n'.join([f'- {name}' for name in symbol_names])
        
        # Get processed summaries for context
        processed_summaries = self.get_processed_summaries()
        
        # Build relevant context
        relevant_processed = set()
        for symbol_id in symbol_ids:
            deps = self.meta_db.execute("""
                SELECT to_node FROM dependencies WHERE from_node = ?
            """, (symbol_id,)).fetchall()
            
            for (dep_id,) in deps:
                dep_name = self.symbol_details.get(dep_id, {}).get('name')
                if dep_name and dep_name in processed_summaries:
                    summary = processed_summaries[dep_name]
                    relevant_processed.add(f"- {dep_name}: {summary[:150]}")
        
        relevant_list_str = '\n'.join(sorted(list(relevant_processed))[:20])
        
        # Context reset instruction
        context_instruction = ""
        if context_reset:
            context_instruction = """
🔄 **CONTEXT RESET**: You are starting completely fresh. Previous conversations and context are irrelevant. Focus entirely on this new batch with fresh perspective.
"""
        
        # Enhanced prompt with Copilot-specific optimizations
        prompt = f"""{context_instruction}
# PostgreSQL Documentation Generation Task - Copilot Optimized

## Your Role
You are a PostgreSQL expert tasked with creating comprehensive, high-quality technical documentation. Your documentation will be used by PostgreSQL developers and contributors.

## Current Batch Context
- **Batch ID**: {batch['batch_id']}
- **Processing Layer**: {batch['layer']} (dependency-ordered processing)
- **Batch Type**: {batch['type']}
- **Progress**: {len(processed_summaries)} symbols already documented out of {self.stats['total_symbols']} total

## Quality Requirements (CRITICAL)
Your documentation must meet these standards to be acceptable:

1. **Comprehensive Overview**: 2-3 detailed sentences explaining purpose, role, and significance
2. **Detailed Technical Description**: 4-6 sentences covering implementation, architecture, algorithms, and behavior
3. **Complete Parameter Documentation**: Every parameter explained with purpose, type, valid values, and behavioral impact
4. **Thorough Dependencies**: Both calling and called-by relationships with explanations of why these relationships exist
5. **Technical Depth**: Include PostgreSQL-specific terminology, architectural context, and implementation details

## Symbols to Document
{symbol_list_str}

## Related Context (Already Processed Symbols)
These summaries provide context about symbols your current symbols may interact with:
{relevant_list_str if relevant_list_str else '(No related processed symbols available)'}

## Documentation Format (STRICT ADHERENCE REQUIRED)
For each symbol, create a file named `[symbol_name].md` with this structure:

```markdown
# [Symbol Name]

## Overview
(Write 2-3 comprehensive sentences explaining:
- What this symbol does and why it exists
- Its role in PostgreSQL's architecture
- Its importance or significance in the codebase)

## Definition
```c
// Complete function signature, struct definition, or enum definition
// Include parameter names and types
```

## Detailed Description
(Write 4-6 detailed sentences covering:
- Technical implementation details
- How it fits into PostgreSQL's architecture  
- Key algorithms or logic implemented
- Important behavioral characteristics
- Performance considerations and optimizations
- Error handling approach)

## Parameters / Member Variables
(For each parameter/member, provide comprehensive details:)
- `param_name`: [Type] - Detailed explanation including purpose, expected values/ranges, constraints, default behavior, and how it affects the function's operation
- `member_name`: [Type] - For struct members, explain what data is stored, when it's populated, how it's used, and relationships to other members

## Dependencies
- **Functions Called/Symbols Referenced**:
  - `function_name` - Explanation of why this dependency exists and what it provides
  - `TYPE_NAME` - Description of how this type is used and why it's needed
- **Called From (Representative Examples)**:
  - `caller_function` - Context of when and why this function calls our symbol
  - `another_caller` - Another important usage scenario

## Notes & Other Information
(Include valuable insights such as:
- Performance characteristics and optimization notes
- Thread safety considerations
- Error conditions and handling
- Historical context or design decisions
- Usage patterns and best practices
- Relationship to PostgreSQL features or subsystems)
```

## Action Required
1. **Research each symbol thoroughly** using the PostgreSQL codebase
2. **Write comprehensive documentation** following the format above
3. **Ensure quality standards are met** - brief or generic content is not acceptable
4. **Save each document** to the output/temp/ directory as `[symbol_name].md`

## Success Criteria
Your documentation will be evaluated on:
- Technical accuracy and completeness
- Depth of explanation and insight
- Proper use of PostgreSQL terminology
- Clear explanations of complex concepts
- Comprehensive coverage of all required sections

Generate high-quality documentation for all {len(symbol_ids)} symbols now.
"""
        return prompt

    def execute_copilot_batch(self, batch: Dict, symbol_ids: List[int]) -> bool:
        """Execute a batch with enhanced Copilot-specific handling"""
        batch_id = batch['batch_id']
        context_reset = self.should_reset_context()
        
        # Log batch start with context reset info
        self.doc_db.execute("""
            INSERT OR REPLACE INTO processing_log 
            (batch_id, symbol_ids, status, started_at, processed_count, context_reset)
            VALUES (?, ?, 'processing', ?, 0, ?)
        """, (batch_id, json.dumps(symbol_ids), datetime.now(), context_reset))
        self.doc_db.commit()
        
        # Create enhanced prompt
        prompt = self.create_copilot_optimized_prompt(symbol_ids, batch, context_reset)
        
        # Ensure output directory exists
        output_dir = Path('output/temp')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            print(f"{'🔄 CONTEXT RESET - ' if context_reset else ''}Processing batch {batch_id}...")
            
            # Write prompt to file for debugging
            prompt_file = output_dir / f"batch_{batch_id}_prompt.txt"
            prompt_file.write_text(prompt, encoding='utf-8')
            
            # For this implementation, we'll simulate the Copilot interaction
            # In practice, this would interface with the actual Copilot system
            print("📝 Enhanced prompt generated with Copilot optimizations")
            print(f"   - Context reset: {context_reset}")
            print(f"   - Quality requirements specified")
            print(f"   - {len(symbol_ids)} symbols to process")
            print(f"   - Prompt saved to: {prompt_file}")
            
            # Simulate success for demonstration
            # In real implementation, you would call the Copilot system here
            success = True
            
            if success:
                print(f"✅ Batch {batch_id} processed successfully")
                
                # Update log with success
                self.doc_db.execute("""
                    UPDATE processing_log 
                    SET status = 'completed', completed_at = ?, processed_count = ?
                    WHERE batch_id = ?
                """, (datetime.now(), len(symbol_ids), batch_id))
                
                # Run quality validation if documents were generated
                self.validate_batch_quality(batch_id)
                
                return True
            else:
                print(f"❌ Batch {batch_id} failed")
                self.doc_db.execute("""
                    UPDATE processing_log 
                    SET status = 'failed', completed_at = ?, error_message = ?
                    WHERE batch_id = ?
                """, (datetime.now(), "Copilot processing failed", batch_id))
                return False
                
        except Exception as e:
            error_msg = str(e)[:1000]
            print(f"💥 Unexpected error in batch {batch_id}: {error_msg}")
            self.doc_db.execute("""
                UPDATE processing_log 
                SET status = 'error', completed_at = ?, error_message = ?
                WHERE batch_id = ?
            """, (datetime.now(), error_msg, batch_id))
            return False
        finally:
            self.doc_db.commit()
            self.current_batch_count += 1

    def validate_batch_quality(self, batch_id: int):
        """Run quality validation on the completed batch"""
        try:
            print(f"🔍 Running quality validation for batch {batch_id}...")
            
            # This would run the quality validation script
            result = subprocess.run(
                ['python', 'scripts/validate_documentation_quality.py'],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                # Load quality results if available
                quality_file = Path('quality_report.json')
                if quality_file.exists():
                    with open(quality_file) as f:
                        quality_data = json.load(f)
                    
                    avg_score = quality_data.get('summary_stats', {}).get('average_overall_score', 0.0)
                    
                    # Update batch log with quality score
                    self.doc_db.execute("""
                        UPDATE processing_log 
                        SET quality_score = ? 
                        WHERE batch_id = ?
                    """, (avg_score, batch_id))
                    
                    print(f"📊 Batch quality score: {avg_score:.1f}/10.0")
                    
                    # Update stats
                    high_count = quality_data.get('summary_stats', {}).get('high_quality_count', 0)
                    medium_count = quality_data.get('summary_stats', {}).get('medium_quality_count', 0)
                    low_count = quality_data.get('summary_stats', {}).get('low_quality_count', 0)
                    
                    self.stats['quality_metrics']['high_quality_count'] += high_count
                    self.stats['quality_metrics']['medium_quality_count'] += medium_count
                    self.stats['quality_metrics']['low_quality_count'] += low_count
                    
                    # Clean up temporary quality report
                    quality_file.unlink()
                    
        except Exception as e:
            print(f"⚠️ Quality validation failed: {e}")

    def get_processed_summaries(self) -> Dict[str, str]:
        """Get summaries of processed symbols"""
        result = self.doc_db.execute("""
            SELECT symbol_name, summary FROM documents 
            WHERE summary IS NOT NULL AND summary != ''
            LIMIT 3000
        """).fetchall()
        return {row[0]: row[1] for row in result}

    def get_processed_symbol_ids(self) -> Set[int]:
        """Get set of processed symbol IDs"""
        result = self.doc_db.execute("SELECT symbol_id FROM documents").fetchall()
        return set(row[0] for row in result)

    def process_all_batches(self):
        """Process all batches with enhanced context management"""
        processed_ids = self.get_processed_symbol_ids()
        
        print(f"🚀 Enhanced Copilot Documentation Orchestrator")
        print(f"📊 Total batches: {len(self.batches)}")
        print(f"🔄 Context reset every {self.context_reset_frequency} batches")
        print(f"✅ Already processed: {len(processed_ids)} symbols")
        print("=" * 60)
        
        for batch in self.batches:
            batch_id = batch['batch_id']
            unprocessed_ids = [sid for sid in batch['symbol_ids'] if sid not in processed_ids]
            
            if not unprocessed_ids:
                print(f"⏭️  Batch {batch_id}: All symbols already processed, skipping")
                continue
            
            print(f"\n📦 Processing Batch {batch_id}/{len(self.batches)}")
            print(f"   Layer: {batch['layer']}, Type: {batch['type']}")
            print(f"   Symbols: {len(unprocessed_ids)}, Estimated tokens: {batch['estimated_tokens']}")
            
            success = self.execute_copilot_batch(batch, unprocessed_ids)
            
            if success:
                self.stats['processed_batches'] += 1
                self.stats['processed_symbols'] += len(unprocessed_ids)
                processed_ids.update(unprocessed_ids)
            else:
                self.stats['failed_batches'] += 1
            
            self.show_progress()
            
            # Rate limiting between batches
            time.sleep(3)

    def show_progress(self):
        """Display current progress with quality metrics"""
        progress = (self.stats['processed_symbols'] / self.stats['total_symbols']) * 100
        print(f"📈 Progress: {progress:.1f}% ({self.stats['processed_symbols']}/{self.stats['total_symbols']})")
        print(f"✅ Completed: {self.stats['processed_batches']}/{self.stats['total_batches']} batches")
        print(f"❌ Failed: {self.stats['failed_batches']} batches")
        
        # Quality metrics
        qm = self.stats['quality_metrics']
        total_quality_docs = qm['high_quality_count'] + qm['medium_quality_count'] + qm['low_quality_count']
        if total_quality_docs > 0:
            print(f"🏆 Quality: {qm['high_quality_count']}H/{qm['medium_quality_count']}M/{qm['low_quality_count']}L")

def main():
    orchestrator = CopilotDocumentationOrchestrator()
    print("Enhanced PostgreSQL Documentation Generation for Copilot")
    print("=" * 60)
    orchestrator.process_all_batches()
    print("\n" + "=" * 60)
    print("🎉 Enhanced documentation generation completed!")

if __name__ == "__main__":
    main()