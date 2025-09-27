# PostgreSQL Streaming Replication Detailed Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive detailed technical documentation for PostgreSQL's Streaming Replication system, focusing on the complete data flow from Primary WAL generation through to Standby replay. This documentation will complement and extend the existing WAL documentation in `topic_specific_generated_docs/about_wal/` by providing deeper implementation details and process-level analysis.

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_symbol_overview(symbol)` - Get concise overview (low context usage)
- `pg_symbol_document(symbol)` - Get detailed documentation
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Available Subagents
1. **streaming-replication-analyzer** - Analyzes streaming replication architecture and data flows
2. **process-flow-documenter** - Documents detailed process flows and inter-process communication
3. **streaming-doc-integrator** - Integrates documentation with existing WAL docs to avoid duplication

### Existing Documentation Context
The project already contains comprehensive WAL documentation in `topic_specific_generated_docs/about_wal/`:
- `wal_complete_documentation.md` - Overall WAL system overview
- `component_replication_sender.md` - Basic sender component documentation
- `component_replication_receiver.md` - Basic receiver component documentation
- `component_recovery.md` - Recovery process documentation

**Critical Requirement**: The new documentation must reference existing documentation appropriately and avoid duplicating content. Instead, it should provide deeper implementation details and process-level analysis that are currently missing.

## Execution Plan

### Stage 1: Streaming Replication Architecture Analysis
Invoke the streaming-replication-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Streaming Replication subsystem with focus on detailed process flows and inter-process communication. Start from these critical entry points:

**Primary Side Processing**:
- WAL Generation to Persistence: XLogInsert, XLogWrite, XLogFlush
- WAL to WalSender Delivery: GetFlushRecPtr, WalSndWakeup, WalSndSetState
- WalSender Transmission: WalSndLoop, XLogSend, WalSndComputeSleeptime

**Standby Side Processing**:
- WalReceiver Operations: WalReceiverMain, XLogWalRcvProcessMsg, XLogWalRcvWrite, XLogWalRcvFlush
- Startup Process Integration: WalRcvForceRestart, WalRcvSetState, XLogWaitForReplayLSN
- WAL Decoding: XLogReadRecord, DecodeXLogRecord, ValidateXLogRecord
- Replay Processing: ApplyWalRecord, PerformWalRecovery, XLogReplay

**Inter-Process Communication**:
- Background Writer interaction during replay
- Shared memory areas and global variables used
- Process wakeup and notification mechanisms
- Standby feedback to Primary (write/flush/reply notifications)

Build a comprehensive dependency map with depth 4 traversal, focusing on:
1. Data structures used for buffering and state management
2. Shared memory regions and their access patterns
3. Process synchronization mechanisms
4. Network protocol message formats and handling
5. Storage constraints and data unit limitations

Generate:
- streaming_architecture_map.json with process-level importance scores
- critical_symbols.txt (top 80 symbols for streaming replication)
- process_flow_outline.md with detailed structure for documentation

Prioritize symbols involved in:
- Critical data persistence paths
- Inter-process communication mechanisms
- Network protocol implementation
- State synchronization and consistency guarantees
- Performance-critical sections
```

**Expected Output Check**: Verify streaming_architecture_map.json contains process-level analysis and identifies critical data flows.

### Stage 2: Detailed Process Flow Documentation
After Stage 1 completes, invoke the process-flow-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed process flow documentation focusing on implementation specifics not covered in existing WAL documentation.

Input files:
- streaming_architecture_map.json
- critical_symbols.txt
- process_flow_outline.md

Documentation Requirements:

**1. Primary Side WAL Processing (detailed_primary_wal_flow.md)**:
- Complete flow from WAL generation to WalSender arrival
- Buffer management and memory copying details
- Timing and synchronization with other processes
- Global variables and shared memory usage
- Performance bottlenecks and optimization points

**2. WalSender Transmission Details (detailed_walsender_processing.md)**:
- Internal buffer management for outgoing data
- Network transmission unit sizes and constraints
- Message format and protocol details
- Synchronous vs asynchronous handling differences
- Client connection management and error recovery

**3. WalReceiver Processing Details (detailed_walreceiver_processing.md)**:
- Data reception unit sizes and buffering strategies
- Storage persistence constraints (KB alignment, write size limits)
- WAL file switching and management
- Startup process notification mechanisms
- Error handling and connection recovery

**4. Startup Process WAL Decoding (detailed_startup_decoding.md)**:
- Specific functions for WAL record file reading
- Checkpoint record handling special cases
- Decoding process implementation details
- Memory management during decoding
- Integration with recovery state machine

**5. Startup Process Replay Operations (detailed_startup_replay.md)**:
- Full-Page Image (FPI) exceptional handling
- WAL record prefetching and read-ahead mechanisms
- Replay process data and state management
- Per-record state updates and progress tracking
- Background Writer coordination during replay

**6. Background Writer Interaction (detailed_bgwriter_interaction.md)**:
- Background Writer behavior during active replay
- Shared buffer management coordination
- Checkpoint coordination with ongoing replay
- Memory pressure handling

**7. Standby Feedback Protocol (detailed_standby_feedback.md)**:
- Write/flush/reply notification implementation
- Message format and transmission details
- Primary side handling of standby feedback
- Performance impact and optimization strategies

Required Diagrams (minimum 8):
- Primary WAL flow sequence (sequenceDiagram)
- WalSender internal state machine (stateDiagram-v2)
- WalReceiver data processing flow (flowchart TD)
- Startup process integration (sequenceDiagram)
- Inter-process communication overview (graph TB)
- Shared memory layout (graph LR)
- Network protocol message flow (sequenceDiagram)
- Background Writer coordination (sequenceDiagram)

For each major component, include:
- Detailed API documentation for key functions
- Data structure layouts and field purposes
- Global variable usage and shared state
- Performance characteristics and bottlenecks
- Error conditions and recovery mechanisms
- Configuration parameters that affect behavior

Code Analysis Focus:
Use pg_symbol_source calls for critical implementation details:
- Buffer management functions
- Network I/O operations
- State synchronization mechanisms
- Error handling paths
```

**Expected Output Check**: Verify detailed implementation coverage and minimum diagram count.

### Stage 3: Integration with Existing Documentation
After Stage 2 completes, invoke the streaming-doc-integrator subagent:

```
Integrate all detailed process flow documentation with existing WAL documentation to create a cohesive, non-duplicative technical resource.

Input files:
- All detailed_*.md files from Stage 2
- All diagrams from Stage 2
- Existing WAL documentation from topic_specific_generated_docs/about_wal/

Integration Requirements:

**1. Reference Mapping**:
- Create comprehensive cross-reference system with existing WAL docs
- Replace any duplicated content with appropriate references
- Maintain conceptual flow while avoiding redundancy
- Add "See Also" sections linking to existing comprehensive coverage

**2. Documentation Structure**:
```
streaming_replication_detailed/
├── index.md                           # Navigation hub with links to existing WAL docs
├── overview_and_scope.md             # Scope definition and relationship to existing docs
├── primary_side_processing/
│   ├── wal_generation_to_walsender.md
│   └── walsender_transmission.md
├── standby_side_processing/
│   ├── walreceiver_operations.md
│   ├── startup_decoding_process.md
│   └── startup_replay_process.md
├── inter_process_coordination/
│   ├── bgwriter_integration.md
│   └── standby_feedback_protocol.md
├── implementation_details/
│   ├── data_structures_and_globals.md
│   ├── shared_memory_layout.md
│   ├── network_protocol_details.md
│   └── performance_constraints.md
├── diagrams/
│   └── [all mermaid diagrams]
└── appendices/
    ├── symbol_reference.md
    ├── configuration_parameters.md
    └── debugging_guide.md
```

**3. Content Enhancement**:
- Add implementation-specific details missing from existing docs
- Include concrete examples with actual data sizes and constraints
- Provide debugging and troubleshooting guidance
- Add performance tuning recommendations based on implementation analysis

**4. Quality Assurance**:
- Verify all critical_symbols.txt entries are covered
- Ensure no functional duplication with existing WAL documentation
- Validate that new content adds genuine implementation value
- Check cross-references to existing documentation are accurate
- Confirm technical accuracy against source code

**5. Final Deliverables**:
- streaming_replication_implementation_guide.md (main technical guide)
- streaming_replication_debugging_reference.md (troubleshooting guide)
- streaming_replication_performance_tuning.md (optimization guide)
- implementation_coverage_report.md (what's new vs existing docs)

**6. Integration Notes**:
For each section, clearly indicate:
- What aspects are covered in existing WAL documentation
- What new implementation details are provided
- How the detailed information complements existing coverage
- References to specific sections in existing documentation
```

**Expected Output Check**: Verify integration quality and added value over existing documentation.

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

### Documentation Integration Guidelines
- **Never duplicate existing content**: Always reference existing WAL documentation when appropriate
- **Add genuine value**: Focus on implementation details not covered in existing docs
- **Maintain coherence**: Ensure new documentation flows logically with existing materials
- **Cross-reference extensively**: Create bidirectional links between new and existing content

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, etc.>
Integration points with existing docs: <count>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical process flows are documented with implementation details
2. Count and list all generated diagrams (must be ≥8)
3. Check integration quality with existing WAL documentation
4. Ensure no broken references or TODO markers remain
5. Confirm that new content provides genuine added value
6. Validate technical accuracy against actual source code

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Detailed streaming replication implementation documentation generated
- [ ] Minimum 8 technical diagrams included
- [ ] Proper integration with existing WAL documentation achieved
- [ ] Implementation details and constraints documented
- [ ] Process-level analysis complete
- [ ] Documentation provides genuine added value over existing materials

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages - proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Streaming Replication Detailed Documentation Generation - Stage 1: Architecture Analysis"