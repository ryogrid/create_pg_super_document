# PostgreSQL Synchronous Streaming Replication WAL Processing Flow Documentation Generation Task

## Objective
Generate comprehensive implementation-level documentation that explains the complete data flow in PostgreSQL's synchronous streaming replication, from WAL generation in backend processes through to client response delivery.

## Prerequisites
- Synchronous streaming replication configuration (`synchronous_commit = on` or `remote_apply`)
- One primary server with one or more standby servers
- `synchronous_standby_names` configured

## Target Audience
- Developers seeking to understand PostgreSQL internals
- DBAs troubleshooting replication issues
- High-availability system architects

---

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_symbol_overview(symbol)` - Get concise overview (low context usage)
- `pg_symbol_document(symbol)` - Get detailed documentation
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Document Structure

### Chapter 1: Architecture Overview
Provide a bird's-eye view of synchronous streaming replication.

**Required Content:**
- Primary server process architecture (backend processes, WAL writer, walsender)
- Standby server process architecture (walreceiver, startup process)
- Key shared memory structures (WAL buffers, XLogCtl, WalSnd array)
- Conceptual diagram of synchronization wait points

**Required Diagram:**
```
[Figure 1: Synchronous Replication Overall Architecture]
- graph TB format
- Express the cycle: Backend Process → WAL Buffer → Disk → walsender → Network → walreceiver → Standby Disk → Acknowledgment → walsender → Backend Process Release
```

---

### Chapter 2: WAL Generation and LSN Assignment (Backend Process)

**2.1 WAL Record Generation Initiation**

Target Symbols for Analysis:
- `XLogInsert` - WAL insertion entry point
- `XLogRecordAssemble` - WAL record assembly
- `XLogInsertRecord` - Actual insertion processing

**Required Explanation Points:**

1. **Timing of LSN Assignment**
   - LSN reservation mechanism within `XLogInsertRecord`
   - Role of `XLogCtl->Insert.CurrBytePos`
   - Exact code point where LSN becomes associated with WAL record
   - Conflict resolution when multiple backends insert WAL simultaneously

2. **Writing to WAL Buffer**
   - WAL buffer page structure
   - `XLogCtl->Insert` lock (LWLock) acquisition timing and granularity
   - Handling records that span WAL page boundaries

**Required Diagram:**
```
[Figure 2: LSN Assignment Sequence Diagram]
- sequenceDiagram format
- Interactions between Backend, XLogInsert, WALBuffer, XLogCtl
- Clearly indicate the timing when LSN is determined
```

---

### Chapter 3: WAL Persistence Processing (Write/Sync)

**3.1 Write Processing Details**

Target Symbols for Analysis:
- `XLogWrite` - Core WAL write function
- `XLogBackgroundFlush` - Background flush
- `issue_xlog_fsync` - fsync invocation

**Required Explanation Points:**

1. **Exclusive Control for Write Processing**
   - Role and acquisition timing of `WALWriteLock`
   - Serialization of Write requests from multiple processes
   - Whether other backends can insert WAL during Write processing

2. **Range of WAL Records Handled in a Single Write/Sync Operation**
   - Relationship between `XLogCtl->LogwrtRqst` and `XLogCtl->LogwrtResult`
   - Mechanism for batching Write requests
   - How "from where to where" is determined
   - Implementation of group commit

3. **Sync Processing Details**
   - Branching by `wal_sync_method` (fsync, fdatasync, open_sync, etc.)
   - `XLogCtl->LogwrtResult.Flush` update after sync completion
   - Mechanism for notifying walsender of sync completion

**Required Diagrams:**
```
[Figure 3: WAL Write/Sync Processing Flow]
- flowchart format
- Flow of lock acquisition, Write range determination, actual I/O, lock release, result update
- Include group commit scenario with multiple backends
```

```
[Figure 4: WAL Buffer and Disk State Transitions]
- Page states within WAL buffer
- Relationship between LogwrtRqst.Write, LogwrtRqst.Flush, LogwrtResult.Write, LogwrtResult.Flush
```

---

### Chapter 4: WAL Transmission by walsender

**4.1 Acquiring WAL Data for Transmission**

Target Symbols for Analysis:
- `WalSndLoop` - Main loop
- `XLogSendPhysical` - Physical WAL transmission
- `WalSndWaitForWal` - WAL waiting

**Required Explanation Points:**

1. **Flow from Sync Completion to walsender Acquisition**
   - Mechanism by which walsender detects flushed WAL
   - Timing of `XLogCtl->LogwrtResult.Flush` reference
   - walsender wakeup triggers (latch, timeout)
   - Method for determining transmittable WAL range

2. **Detailed walsender Processing Flow**
   - `WalSndLoop` state machine
   - Order of processing in each iteration
   - `WalSndState` state transitions

**Required Diagram:**
```
[Figure 5: walsender State Transition Diagram]
- stateDiagram-v2 format
- States such as STARTUP, CATCHUP, STREAMING, STOPPING and transition conditions
```

**4.2 Data Transmission Units and Format**

Target Symbols for Analysis:
- `XLogSendPhysical` - Physical WAL transmission implementation
- `pq_putmessage_noblock` - Message transmission

**Required Explanation Points:**

1. **Hierarchical Structure of Transmission Data Units**
   - WAL record vs WAL page vs transmission chunk
   - Factors determining data size per transmission
   - Role of `MAX_SEND_SIZE`

2. **CopyData Message Structure**
   - Message header format
   - WAL data payload structure
   - Relationship between message boundaries and WAL record boundaries (asynchrony)

3. **Flow Control**
   - Role of `wal_sender_timeout`
   - Processing when send buffer is full
   - Transmission adjustment according to standby processing speed
   - Wait conditions in `WalSndWaitForWal`

**Required Diagrams:**
```
[Figure 6: walsender Transmission Data Structure]
- Illustrate relationships between WAL segment, WAL page, WAL record, CopyData message
```

```
[Figure 7: walsender Processing Sequence (Single Iteration)]
- sequenceDiagram format
- Details of WAL reading, packet construction, transmission, response confirmation
```

---

### Chapter 5: Keep-Alive and Standby State Monitoring

**5.1 Keep-Alive Transmission**

Target Symbols for Analysis:
- `WalSndKeepalive` - keepalive transmission
- `WalSndKeepaliveIfNecessary` - transmission decision

**Required Explanation Points:**
- Triggers for keepalive transmission (timeout, explicit request)
- keepalive message structure
- Meaning and use cases of `replyRequested` flag

**5.2 Periodic Status Notifications**

Target Symbols for Analysis:
- `ProcessStandbyReplyMessage` - Response processing
- `ProcessStandbyHSFeedbackMessage` - Hot Standby Feedback

**Required Explanation Points:**
- Types of notifications sent by walsender
- Transmission intervals and triggers for each notification
- Bidirectional communication patterns with standby

---

### Chapter 6: Processing Responses from Standby

**6.1 Receiving and Parsing Response Messages**

Target Symbols for Analysis:
- `ProcessRepliesIfAny` - Response processing entry point
- `ProcessStandbyReplyMessage` - Response message parsing
- `WalSndWaitForWal` - WAL waiting (including response processing)

**Required Explanation Points:**

1. **Response Message Structure**
   - Meaning of each field in `StandbyReplyMessage`
   - Differences between `writePtr`, `flushPtr`, `applyPtr`
   - Purpose of timestamps

2. **Response Processing Flow in walsender**
   - Timing of response reception (polling, event-driven)
   - Updates to `WalSnd->write`, `WalSnd->flush`, `WalSnd->apply`
   - Logic for synchronous standby determination

**Required Diagram:**
```
[Figure 8: Standby Response Processing Sequence]
- sequenceDiagram format
- Flow: walreceiver → walsender → SyncRepWaitForLSN release
```

---

### Chapter 7: Synchronous Replication Wait and Release

**7.1 Backend Process Waiting**

Target Symbols for Analysis:
- `SyncRepWaitForLSN` - Core synchronous wait function
- `SyncRepQueueInsert` - Insertion into wait queue
- `SyncRepWakeQueue` - Wait release

**Required Explanation Points:**

1. **Conditions for Starting Wait**
   - Branching by `synchronous_commit` setting value
   - Determination of LSN requiring wait
   - Structure of wait queue (`SyncRepQueue`)

2. **Wait State Management**
   - State transitions of `MyProc->syncRepState`
   - Processing performed by waiting process (latch waiting)
   - Timeout handling

**7.2 Wait Release Triggers**

Target Symbols for Analysis:
- `SyncRepReleaseWaiters` - Wait release
- `SyncRepGetSyncStandbysPriority` / `SyncRepGetSyncStandbysQuorum` - Synchronous standby determination

**Required Explanation Points:**

1. **Release Condition Determination**
   - `sync_standby_priority` vs `sync_standby_quorum` modes
   - Determination logic when multiple standbys exist
   - Role of `WalSndCtl->lsn[]` array

2. **Release Processing Flow**
   - When walsender calls `SyncRepReleaseWaiters`
   - Removal from wait queue and latch setting
   - Notification mechanism to backend process

**Required Diagrams:**
```
[Figure 9: Synchronous Replication Wait/Release Sequence]
- sequenceDiagram format
- Backend → Wait Queue → walsender → Standby → Response → walsender → Wait Release → Backend Resume
```

```
[Figure 10: SyncRepQueue State Transitions]
- stateDiagram-v2 format
- SYNC_REP_NOT_WAITING, SYNC_REP_WAITING, SYNC_REP_WAIT_COMPLETE
```

---

### Chapter 8: Response to Client

**8.1 From Commit Completion to Response**

Target Symbols for Analysis:
- `CommitTransaction` - Transaction commit
- `RecordTransactionCommit` - Commit record recording
- `FinishPreparedTransaction` - 2PC completion processing

**Required Explanation Points:**
- Processing after returning from `SyncRepWaitForLSN`
- Client notification of commit completion
- Rollback processing on error

**Required Diagram:**
```
[Figure 11: Complete Transaction Commit Sequence]
- sequenceDiagram format
- Client → Backend → WAL Generation → Persistence → walsender → Standby → Response → Wait Release → Client Response
- Express entire flow in one diagram
```

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the **architecture-analyzer** subagent with the following instruction:

```
Analyze the PostgreSQL synchronous streaming replication subsystem architecture starting from these entry points:

Primary WAL Path:
- XLogInsert, XLogInsertRecord, XLogRecordAssemble
- XLogWrite, XLogFlush, XLogBackgroundFlush
- issue_xlog_fsync

Synchronous Replication Path:
- WalSndMain, WalSndLoop, WalSndWaitForWal
- XLogSendPhysical, WalSndKeepalive
- ProcessStandbyReplyMessage, ProcessRepliesIfAny

Synchronous Wait/Release Path:
- SyncRepWaitForLSN, SyncRepQueueInsert
- SyncRepReleaseWaiters, SyncRepWakeQueue

Build a comprehensive dependency map with depth 3 traversal. Focus on:
1. LSN assignment and WAL buffer management
2. Write/Sync processing and exclusive control mechanisms
3. walsender main loop and data transmission logic
4. Standby response processing and synchronous wait release
5. Shared memory structures (XLogCtl, WalSndCtl, SyncRepQueue)

Generate:
- architecture_map.json with importance scores (0.0-1.0)
- key_symbols.txt (top 40 symbols prioritized by relevance to sync replication)
- shared_memory_structures.md (detailed analysis of XLogCtl, WalSndCtl, WalSnd)
- initial_outline.md with suggested documentation structure

Prioritize symbols involved in:
- LSN assignment timing
- WALWriteLock and other exclusive control
- Group commit implementation
- walsender state machine
- Synchronous wait queue management
```

**Expected Output Check**: 
- Verify architecture_map.json contains at least 60 symbols
- Verify key_symbols.txt includes all 7 Tier 1 symbols
- Verify shared_memory_structures.md documents XLogCtlData, WalSndCtlData, WalSnd

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the **detail-documenter** subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the synchronous streaming replication WAL processing flow.

Input files:
- architecture_map.json
- key_symbols.txt
- shared_memory_structures.md
- initial_outline.md

Documentation Requirements:

1. For Tier 1 symbols (MUST analyze with pg_symbol_source):
   - XLogInsertRecord: LSN assignment mechanism, CurrBytePos usage
   - XLogWrite: Write processing, WALWriteLock, range determination
   - XLogSendPhysical: Data transmission units, MAX_SEND_SIZE
   - ProcessStandbyReplyMessage: Response parsing, LSN updates
   - SyncRepWaitForLSN: Wait queue insertion, latch waiting
   - SyncRepReleaseWaiters: Release condition check, queue processing
   - WalSndLoop: Main loop structure, state machine

2. For each Tier 1 symbol, document:
   - Function signature and parameters
   - Step-by-step internal logic with code references
   - Lock acquisition/release points
   - Interaction with shared memory structures
   - Error handling paths

3. Required Diagrams (minimum 11):
   - Figure 1: Overall architecture (graph TB)
   - Figure 2: LSN assignment sequence (sequenceDiagram)
   - Figure 3: WAL Write/Sync flow (flowchart)
   - Figure 4: WAL buffer state transitions (diagram)
   - Figure 5: walsender state machine (stateDiagram-v2)
   - Figure 6: Transmission data structure (diagram)
   - Figure 7: walsender single iteration (sequenceDiagram)
   - Figure 8: Standby response processing (sequenceDiagram)
   - Figure 9: Sync wait/release sequence (sequenceDiagram)
   - Figure 10: SyncRepQueue states (stateDiagram-v2)
   - Figure 11: Complete commit sequence (sequenceDiagram)

4. Special Focus Areas:
   - Exact timing of LSN assignment in XLogInsertRecord
   - WALWriteLock acquisition and release boundaries
   - How LogwrtRqst and LogwrtResult coordinate Write/Sync
   - CopyData message vs WAL page vs WAL record boundaries
   - Flow control mechanisms in walsender
   - Transition from WalSnd->flush update to SyncRepReleaseWaiters call

5. Code Analysis Guidelines:
   - Use pg_symbol_source for all Tier 1 symbols
   - Extract relevant code snippets (max 50 lines each)
   - Add inline comments explaining key logic
   - Reference specific line numbers

Generate chapter files:
- chapter_01_architecture.md
- chapter_02_wal_generation_lsn.md
- chapter_03_wal_persistence.md
- chapter_04_walsender_transmission.md
- chapter_05_keepalive_monitoring.md
- chapter_06_standby_response.md
- chapter_07_sync_wait_release.md
- chapter_08_client_response.md
- diagrams/*.mermaid (11 files)
```

**Expected Output Check**: 
- Ensure all 8 chapter files are generated
- Verify all 11 diagram files exist
- Confirm Tier 1 symbols have source code analysis

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the **integration-optimizer** subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

Input files:
- All chapter_*.md files from Stage 2
- All diagrams/*.mermaid files
- architecture_map.json for cross-reference
- shared_memory_structures.md

Integration Requirements:

1. Document Structure:
   - index.md: Table of contents with navigation links
   - Executive Summary: 1-page overview of sync replication data flow
   - 8 main chapters (preserve chapter structure from Stage 2)
   - Appendix A: Symbol Index (alphabetical, with chapter references)
   - Appendix B: Glossary (LSN, WAL, sync rep terminology)
   - Appendix C: Configuration Parameters Reference

2. Enhancement Tasks:
   - Add cross-references between related sections
   - Ensure consistent terminology (use PostgreSQL official terms)
   - Add "Key Takeaways" summary at end of each chapter
   - Insert diagram references in appropriate locations
   - Add navigation links (Previous/Next chapter)

3. Quality Assurance:
   - Verify all 7 Tier 1 symbols are documented with source analysis
   - Ensure all 11 diagrams are referenced in text
   - Check logical flow from WAL generation to client response
   - Validate all internal links work
   - Confirm no TODO markers remain

4. Specific Validations:
   - [ ] LSN assignment timing is explicitly explained with code reference
   - [ ] WALWriteLock scope and group commit mechanism documented
   - [ ] Write/Sync range determination logic explained
   - [ ] CopyData/WALpage/WALrecord relationship clarified
   - [ ] Flow control mechanisms documented
   - [ ] Complete path from standby response to backend release traced

5. Output Organization:
   output/
   ├── index.md
   ├── 01_architecture_overview.md
   ├── 02_wal_generation_lsn.md
   ├── 03_wal_persistence.md
   ├── 04_walsender_transmission.md
   ├── 05_keepalive_monitoring.md
   ├── 06_standby_response.md
   ├── 07_sync_wait_release.md
   ├── 08_client_response.md
   ├── diagrams/
   │   └── (11 .mermaid files)
   ├── appendix_symbol_index.md
   ├── appendix_glossary.md
   └── appendix_config_params.md

6. Generate Quality Report:
   - quality_report.md containing:
     * Symbol coverage metrics
     * Diagram count verification
     * Cross-reference validation results
     * Any gaps or areas needing improvement
```

**Expected Output Check**: 
- Verify all output files exist
- Confirm quality_report.md shows >90% coverage
- Validate navigation links work

---

## MCP Server Usage Guidelines

### Symbol Investigation Priority

**Tier 1 - Required (Detailed Source Code Analysis):**
```
XLogInsertRecord           # Core of LSN assignment
XLogWrite                  # Write processing implementation
XLogSendPhysical           # WAL transmission implementation
ProcessStandbyReplyMessage # Response processing
SyncRepWaitForLSN          # Core of synchronous wait
SyncRepReleaseWaiters      # Core of wait release
WalSndLoop                 # walsender main loop
```

**Tier 2 - Important (Overview and References):**
```
WalSndWaitForWal
WalSndKeepalive
XLogBackgroundFlush
SyncRepQueueInsert
SyncRepWakeQueue
XLogFlush
issue_xlog_fsync
```

**Tier 3 - Supplementary (As Needed):**
```
XLogRecordAssemble
XLogInsert
WalSndState (enum)
SyncRepStandbyData (struct)
XLogCtlData (struct)
WalSndCtlData (struct)
WalSnd (struct)
StandbyReplyMessage (struct)
```

### Investigation Procedure
1. For each Tier 1 symbol, use `pg_symbol_source` to get complete source
2. Use `pg_references_from` to understand callees
3. Use `pg_references_to` to understand callers (context)
4. For structures, use `pg_symbol_document` for detailed field descriptions
5. Limit source retrieval to most relevant 100 lines per symbol

---

## Orchestration Rules

### Execution Flow
1. Execute each stage sequentially - do not proceed until previous stage completes successfully
2. Capture all output files from each subagent
3. Validate expected outputs before proceeding to next stage
4. Report progress after each stage

### Error Handling
- **Subagent failure**: Retry once with modified parameters, then proceed with partial results
- **Missing expected files**: Log warning, attempt recovery using available data
- **Context limit approached**: Save progress, prioritize Tier 1 symbols, defer Tier 3
- **MCP server errors**: Implement exponential backoff (1s, 2s, 4s) for retries

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, etc.>
Validation: <pass/fail for expected outputs>
Next stage: <description>
```

### Final Validation Checklist
Before declaring completion:
- [ ] All 3 stages executed successfully
- [ ] All 7 Tier 1 symbols documented with source analysis
- [ ] All 11 diagrams generated and referenced
- [ ] LSN assignment timing explicitly documented
- [ ] Write/Sync exclusive control mechanism documented
- [ ] WAL record range per persistence operation documented
- [ ] walsender data units (CopyData, page, record) explained
- [ ] Flow control mechanism documented
- [ ] Complete flow from standby response to backend release documented
- [ ] Quality report shows >90% symbol coverage
- [ ] No broken links or TODO markers

---

## Quality Requirements

### Mandatory Conditions
- [ ] All 11 diagrams (Figures 1-11) are included
- [ ] All 7 Tier 1 symbols have implementation-level explanations
- [ ] LSN assignment timing is clearly explained
- [ ] Write/Sync exclusive control is detailed
- [ ] WAL record range determination logic per persistence operation is explained
- [ ] Relationship between walsender data units (CopyData, page, record) is explained
- [ ] Flow control mechanism is explained
- [ ] Complete flow from standby response to backend release is explained

### Recommended Conditions
- [ ] Each chapter contains actual source code excerpts with comments
- [ ] Performance considerations are documented
- [ ] Troubleshooting hints are included
- [ ] Relationship between configuration parameters and behavior is explained

---

## Output Format

### File Structure
```
output/
├── index.md                           # Table of contents and navigation
├── 01_architecture_overview.md        # Chapter 1
├── 02_wal_generation_lsn.md          # Chapter 2
├── 03_wal_persistence.md             # Chapter 3
├── 04_walsender_transmission.md      # Chapter 4
├── 05_keepalive_monitoring.md        # Chapter 5
├── 06_standby_response.md            # Chapter 6
├── 07_sync_wait_release.md           # Chapter 7
├── 08_client_response.md             # Chapter 8
├── diagrams/
│   ├── 01_overall_architecture.mermaid
│   ├── 02_lsn_assignment_sequence.mermaid
│   ├── 03_wal_write_sync_flow.mermaid
│   ├── 04_wal_buffer_state.mermaid
│   ├── 05_walsender_state.mermaid
│   ├── 06_send_data_structure.mermaid
│   ├── 07_walsender_iteration.mermaid
│   ├── 08_standby_response_sequence.mermaid
│   ├── 09_sync_wait_release_sequence.mermaid
│   ├── 10_syncrep_queue_state.mermaid
│   └── 11_complete_commit_sequence.mermaid
├── appendix_symbol_index.md          # Symbol index
├── appendix_glossary.md              # Glossary
├── appendix_config_params.md         # Configuration parameters
└── quality_report.md                 # Coverage and quality metrics
```

### Chapter Template
```markdown
# Chapter N: [Chapter Title]

## Overview
[Summary of content covered in this chapter]

## Processing Flow
[Explanation of main processing flow]

## Implementation Details

### [Subsection]
[Detailed implementation explanation]

#### Related Symbols
- `symbol_name`: [Role description]

#### Source Code Excerpt
```c
// filename:line_number
[Relevant code excerpt with comments]
```

## Diagrams
[References to related diagrams with explanations]

## Configuration Parameters
| Parameter | Default | Impact |
|-----------|---------|--------|
| ... | ... | ... |

## Key Takeaways
- [Summary point 1]
- [Summary point 2]

## Navigation
← [Previous: Chapter N-1](link) | [Next: Chapter N+1](link) →
```

---

## Start Execution

Begin with Stage 1 immediately. Do not wait for confirmation between stages - proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Synchronous Streaming Replication WAL Documentation Generation - Stage 1: Architecture Analysis"