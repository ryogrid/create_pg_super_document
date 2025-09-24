# PostgreSQL Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's Checkpointing system, covering the complete lifecycle from checkpoint initiation through buffer flushing, WAL coordination, recovery point establishment and especially FPW technique for covering torn page.

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Checkpointing subsystem architecture starting from these entry points:
- Checkpoint Initiation: CreateCheckPoint, RequestCheckpoint, CheckpointerMain
- Buffer Management: BufferSync, SyncOneBuffer, FlushBuffer
- WAL Coordination: UpdateControlFile, UpdateMinRecoveryPoint, LogCheckpointStart, LogCheckpointEnd
- Background Writer: BackgroundWriterMain, BgBufferSync
- Recovery Points: CreateRestartPoint, UpdateCheckPointDistanceEstimate

Build a comprehensive dependency map with depth 5 traversal. Focus on:
1. Checkpoint triggering mechanisms (time-based, WAL-based, manual)
2. Dirty buffer identification and flushing strategies
3. WAL-checkpoint coordination and consistency
4. Full page write (FPW) handling for torn page prevention
5. Background writer integration and pacing
6. Recovery point creation and control file updates

Generate:
- architecture_map.json with importance scores
- key_symbols.txt (top 30 symbols)
- initial_outline.md with suggested documentation structure

Prioritize symbols involved in:
- Checkpoint scheduling and triggering
- Buffer pool scanning and flushing
- FPW logic and implementation
- I/O throttling and spreading
- WAL segment recycling
- Control file management
```

**Expected Output Check**: Verify architecture_map.json contains at least 50 symbols and identifies 5+ critical paths.

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the Checkpointing subsystem.

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
   - Overall checkpoint architecture (graph TB)
   - Checkpoint triggering decision flow (flowchart LR)
   - Buffer flushing sequence (sequenceDiagram)
   - Checkpoint states and transitions (stateDiagram-v2)
   - WAL-checkpoint coordination timeline (sequenceDiagram)
   - FPW logic through checkpoints at diffent times (flowchart LR)

3. Special Focus Areas:
   - Checkpoint request flags and modes
   - Full page writes and torn page prevention   
   - Checkpoint completion criteria
   - Control file structure and updates
   - Checkpoint spreading and completion target
   - Incremental checkpointing strategies

4. Code Analysis Priorities:
   - Focus pg_symbol_source calls on:
     * CreateCheckPoint (main checkpoint logic)
     * BufferSync (buffer flushing orchestration)
     * CheckpointerMain (checkpointer process main loop)
     * LogCheckpointStart/End (WAL integration)
     * UpdateControlFile (persistence mechanism)

Generate component files organized by subsystem:
- component_checkpoint_control.md
- component_buffer_flushing.md
- component_wal_coordination.md
- component_background_writer.md
- component_recovery_points.md
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
   - Executive Summary (1 page): Checkpointing's role in durability and recovery
   - Architecture Overview: System-wide perspective with main diagram
   - Core Components: Organized by functional areas
   - Deep Dives: Complex topics like FPW handling, I/O spreading
   - Appendices: Symbol index, glossary, further reading

2. Enhancement Tasks:
   - Generate comprehensive cross-references
   - Eliminate redundancy while maintaining clarity
   - Standardize terminology (prefer PostgreSQL official terms)


3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented
   - Ensure logical flow from high-level to implementation details
   - Check diagram rendering and placement
   - Confirm code examples match actual source

4. Output Organization:
   If total size > 2000 lines:
   - Split into logical modules
   - Create index.md as navigation hub
   - Maintain coherent reading experience
   
   Otherwise:
   - Single checkpointing_complete_documentation.md
   - Internal navigation via TOC

5. Additional Deliverables:
   - checkpointing_quick_reference.md (2-page summary)
   - checkpointing_api_reference.md (function signatures)
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
- [ ] Comprehensive checkpointing documentation generated
- [ ] Minimum 5 technical diagrams included
- [ ] Quality report shows >80% symbol coverage
- [ ] Both high-level overview and deep technical details are present

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages - proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Checkpointing Documentation Generation - Stage 1: Architecture Analysis"
