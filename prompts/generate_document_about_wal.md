# PostgreSQL Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's WAL (Write-Ahead Logging) system, covering the complete lifecycle from generation through persistence, streaming replication to standby servers, and standby processing.

## Available Resources

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL WAL subsystem architecture starting from these entry points:
- Primary WAL Generation: XLogInsert, XLogWrite, XLogFlush
- Streaming Replication: WalSndLoop, WalSndMain, WalSenderMain
- Standby Processing: WalReceiverMain, WalRcvStreamStart, XLogWalRcvProcessMsg
- Recovery: StartupXLOG, PerformWalRecovery, ApplyWalRecord

Build a comprehensive dependency map with depth 3 traversal. Focus on:
1. Transaction log generation and buffering
2. Synchronous vs asynchronous replication paths  
3. Standby feedback mechanisms
4. Recovery and replay processes
5. Checkpoint coordination

Generate:
- architecture_map.json with importance scores
- key_symbols.txt (top 30 symbols)
- initial_outline.md with suggested documentation structure

Prioritize symbols involved in:
- Critical write paths
- Replication protocol
- Consistency guarantees
- Performance bottlenecks
```

**Expected Output Check**: Verify architecture_map.json contains at least 50 symbols and identifies 3+ critical paths.

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the WAL subsystem.

Input files:
- architecture_map.json
- key_symbols.txt
- initial_outline.md

Documentation Requirements:
1. For each symbol with importance > 0.8:
   - Complete API documentation
   - Internal logic explanation
   - Integration patterns
   - Performance characteristics

2. Required Diagrams (minimum 5):
   - Overall WAL architecture (graph TB)
   - WAL record generation sequence (sequenceDiagram)
   - Replication data flow (flowchart LR)
   - Standby state machine (stateDiagram-v2)
   - Recovery process flow (sequenceDiagram)

3. Special Focus Areas:
   - XLogRecord structure and variations
   - LSN (Log Sequence Number) management
   - Full-page writes optimization
   - Replication slot mechanics
   - Timeline switching during recovery

4. Code Analysis Priorities:
   - Reading source code for:
     * XLogInsert (insertion logic)
     * WalSndLoop (main replication loop)
     * ApplyWalRecord (replay mechanism)
   - Limit code retrieval to 100 lines per symbol

Generate component files organized by subsystem:
- component_wal_generation.md
- component_wal_writing.md
- component_replication_sender.md
- component_replication_receiver.md
- component_recovery.md
- diagrams/*.mermaid
```

**Expected Output Check**: Ensure all Tier 1 symbols have detailed documentation and verify minimum diagram count.

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

Input files:
- All component_*.md files from Stage 2
- All diagrams/*.mermaid files
- architecture_map.json for reference

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): WAL's role in ACID compliance
   - Quick Start: Common scenarios and configurations
   - Architecture Overview: System-wide perspective with main diagram
   - Core Components: Organized by lifecycle stage
   - Deep Dives: Complex topics like group commit, parallel apply
   - Appendices: Symbol index, glossary, further reading

2. Enhancement Tasks:
   - Generate comprehensive cross-references
   - Eliminate redundancy while maintaining clarity
   - Standardize terminology (prefer PostgreSQL official terms)
   - Add navigation aids (TOC, breadcrumbs, next/prev links)

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented
   - Ensure logical flow from high-level to implementation details
   - Validate all internal links
   - Check diagram rendering and placement
   - Confirm code examples match actual source

4. Output Organization:
   If total size > 2000 lines:
   - Split into logical modules
   - Create index.md as navigation hub
   - Maintain coherent reading experience
   
   Otherwise:
   - Single wal_complete_documentation.md
   - Internal navigation via TOC

5. Additional Deliverables:
   - wal_quick_reference.md (2-page summary)
   - wal_api_reference.md (function signatures)
   - quality_report.md (coverage metrics, suggestions)
```

**Expected Output Check**: Verify professional documentation quality and complete coverage.

## Orchestration Rules

### Execution Flow
1. Execute each stage sequentially - do not proceed until previous stage completes successfully
2. Capture all output files from each subagent
3. Validate expected outputs before proceeding to next stage
4. Report progress after each stage: "[Stage N Complete] Generated: <file list>"

### Error Handling
- **Subagent failure**: Retry once with modified parameters, then proceed with partial results
- **Missing expected files**: Log warning, attempt recovery using available data
- **Context limit reached**: Save progress, split remaining work into smaller chunks
- **MCP server errors**: Implement exponential backoff (1s, 2s, 4s) for retries

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, etc.>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical path symbols are documented
2. Count and list all generated diagrams (must be ≥5)
3. Check total documentation coverage (target >80% of key symbols)
4. Ensure no broken references or TODO markers remain
5. Confirm file organization follows specified structure

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive WAL documentation generated
- [ ] Minimum 5 technical diagrams included
- [ ] Quality report shows >80% symbol coverage
- [ ] Documentation is organized and navigable
- [ ] Both high-level overview and deep technical details are present

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages - proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL WAL Documentation Generation - Stage 1: Architecture Analysis"
