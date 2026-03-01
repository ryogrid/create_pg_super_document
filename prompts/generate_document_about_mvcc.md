# PostgreSQL MVCC Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **MVCC (Multi-Version Concurrency Control)** system, covering the complete lifecycle from transaction initiation through tuple versioning, visibility determination, snapshot management, and garbage collection (VACUUM).

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_overview(symbol)` - Get concise overview (low context usage)
- `pg_symbol_document(symbol)` - Get detailed documentation
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for MVCC documentation:

| Directory | Contents |
|---|---|
| `src/backend/access/heap/` | Heap access methods — `heapam.c`, `heapam_visibility.c` (visibility rules), `vacuumlazy.c` (VACUUM) |
| `src/backend/access/transam/` | Transaction management — `xact.c` (transaction lifecycle), `clog.c` (commit log), `varsup.c` (XID allocation), `slru.c`, `subtrans.c`, `multixact.c` |
| `src/backend/storage/lmgr/` | Lock manager — `lmgr.c`, `lwlock.c`, `predicate.c` (SSI), `proc.c` |
| `src/backend/storage/ipc/` | Shared memory and IPC — `procarray.c` (ProcArray, snapshot support), `sinvaladt.c` |
| `src/backend/storage/buffer/` | Buffer manager — `bufmgr.c` (pin/unpin, buffer interactions with MVCC) |
| `src/backend/utils/time/` | Snapshot management — `snapmgr.c` (snapshot lifecycle) |
| `src/backend/commands/` | `vacuum.c`, `analyze.c` (user-facing VACUUM/ANALYZE commands) |
| `src/backend/postmaster/` | `autovacuum.c` (autovacuum launcher/worker) |
| `src/include/access/` | Key headers — `htup_details.h`, `heapam.h`, `transam.h`, `xact.h`, `visibilitymap.h` |
| `src/include/storage/` | `proc.h` (PGPROC), `procarray.h`, `buf_internals.h`, `lwlock.h` |
| `src/include/utils/` | `snapshot.h` (SnapshotData), `combocid.h` |

**Usage guidelines for source code**:
- **Prefer direct source reading** over MCP `pg_symbol_source()` when exploring file-level structure, neighboring functions, or header definitions. Use `cat`, `grep`, `find`, and `head`/`tail` to navigate the tree.
- **Use MCP tools** for targeted symbol lookups, cross-reference analysis, and pre-indexed documentation.
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions that MCP may not fully index.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/access/heap/heapam_visibility.c:123`) for traceability.

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL MVCC (Multi-Version Concurrency Control) architecture.

Use BOTH the MCP server tools AND the local source tree (`./src/`) for analysis.

**Source exploration strategy for this stage**:
- Start by scanning key directories to identify relevant files:
  - `find ./src/backend/access/heap/ -name '*.c'`
  - `find ./src/backend/access/transam/ -name '*.c'`
  - `find ./src/backend/storage/lmgr/ -name '*.c'`
  - `find ./src/backend/storage/ipc/ -name '*.c'`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols the MCP index might miss.
- Read key header files (`src/include/access/htup_details.h`, `src/include/utils/snapshot.h`, `src/include/storage/proc.h`) to understand data structure definitions.
- Cross-validate MCP `pg_references_from()` / `pg_references_to()` results against `grep` results in the source tree.

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. Transaction lifecycle management
   - Transaction start (StartTransaction, StartTransactionCommand)
   - Commit paths (CommitTransaction, CommitTransactionCommand, RecordTransactionCommit)
   - Abort paths (AbortTransaction, AbortCurrentTransaction)
   - Subtransaction handling (SaveSubTransaction, ReleaseCurrentSubTransaction)

2. Tuple versioning and header management
   - HeapTuple / HeapTupleHeaderData structure
   - t_xmin, t_xmax, t_ctid fields and their roles
   - Tuple insertion, update (HOT updates included), and deletion marking
   - Combo CID (combocid.c) for command-id management within transactions

3. Visibility determination
   - HeapTupleSatisfiesMVCC and related visibility functions (tqual.c / heapam_visibility.c)
   - HTSU_Result status codes
   - Transaction status checks via CLOG (pg_xact)
   - Hint bits optimization

4. Snapshot management
   - SnapshotData structure (xmin, xmax, xip array)
   - GetSnapshotData / GetTransactionSnapshot
   - Snapshot isolation levels (READ COMMITTED vs REPEATABLE READ vs SERIALIZABLE)
   - Active snapshot stack management (PushActiveSnapshot, PopActiveSnapshot)

5. Concurrency control infrastructure
   - PGPROC / PGXACT arrays and shared memory layout
   - LWLock and heavyweight lock interactions with MVCC
   - ProcArray management (ProcArrayAdd, ProcArrayRemove, TransactionIdIsInProgress)
   - Predicate locking for SERIALIZABLE isolation (predicate.c)

6. Garbage collection (VACUUM)
   - Lazy VACUUM workflow (lazy_scan_heap, lazy_vacuum_heap_rel)
   - Dead tuple identification via visibility rules
   - Freeze processing (heap_freeze_tuple, FreezeLimit computation)
   - VACUUM's interaction with running transactions (OldestXmin computation)
   - Autovacuum coordination

7. CLOG (Commit Log) and transaction status persistence
   - Transaction status storage (pg_xact / slru.c)
   - Status transitions: IN_PROGRESS → COMMITTED / ABORTED / SUB_COMMITTED
   - CLOG page management and truncation

Generate:
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 30 symbols ranked by importance)
- initial_outline.md with suggested documentation structure
```

**Expected Output Check**: Verify architecture_map.json contains at least 50 symbols and identifies 5+ critical paths (e.g., transaction commit path, visibility check path, vacuum path, snapshot acquisition path, tuple update path).

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the PostgreSQL MVCC system.

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function implementation from `./src/` and annotate key logic steps.
- When documenting visibility rules, read `src/backend/access/heap/heapam_visibility.c` end-to-end — this is the single most critical file for MVCC understanding.
- When documenting transaction lifecycle, read `src/backend/access/transam/xact.c` focusing on `StartTransaction()`, `CommitTransaction()`, `AbortTransaction()`.
- When documenting VACUUM, read `src/backend/access/heap/vacuumlazy.c` for the lazy vacuum implementation.
- When documenting snapshots, read `src/backend/storage/ipc/procarray.c` (`GetSnapshotData()`) and `src/backend/utils/time/snapmgr.c`.
- For data structure documentation, directly quote struct definitions from header files (e.g., `HeapTupleHeaderData` from `src/include/access/htup_details.h`).
- Include file paths and line numbers in all source references for traceability.
- Use `grep -rn` to find all callers of key functions to document integration patterns accurately.

Input files:
- architecture_map.json
- key_symbols.txt
- initial_outline.md

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics and concurrency implications
   - Key invariants and assumptions

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader MVCC system
   - Key relationships to Tier 1 symbols

3. Required Diagrams (minimum 7):
   - Transaction lifecycle state machine (IN_PROGRESS → COMMITTED/ABORTED)
   - Tuple version chain diagram (INSERT → UPDATE → DELETE with t_xmin/t_xmax)
   - MVCC visibility decision flowchart (HeapTupleSatisfiesMVCC logic)
   - Snapshot acquisition and usage sequence diagram
   - VACUUM dead tuple identification and cleanup flowchart
   - Shared memory layout for MVCC structures (PGPROC, ProcArray, CLOG)
   - Isolation level comparison diagram (READ COMMITTED vs REPEATABLE READ vs SERIALIZABLE)

4. Special Focus Areas (dedicate extra depth):
   - Visibility rules: exhaustive case analysis of HeapTupleSatisfiesMVCC
   - Snapshot mechanics: how xmin/xmax/xip determine tuple visibility
   - Freeze processing: why freezing is needed and how FreezeLimit is computed
   - HOT (Heap-Only Tuple) updates: optimization path and chain management
   - Hint bits: lazy status caching on tuple headers
   - Serializable Snapshot Isolation (SSI): predicate locks and rw-conflict detection

5. Source code references:
   - For each major function, include the relevant source file path
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values

Generate component files organized by functional area:
- component_transaction_lifecycle.md    (start, commit, abort, subtransactions)
- component_tuple_versioning.md         (heap tuple structure, version chains, HOT)
- component_visibility.md               (visibility rules, hint bits, HTSU_Result)
- component_snapshots.md                (snapshot types, acquisition, isolation levels)
- component_concurrency_infra.md        (ProcArray, locking, PGPROC, SSI)
- component_vacuum.md                   (lazy vacuum, freeze, autovacuum, OldestXmin)
- component_clog.md                     (transaction status persistence, SLRU)
- diagrams/*.mermaid
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 7 diagrams are generated.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 10 critical function signatures and struct definitions against `./src/` to ensure accuracy.
- Verify that all quoted code snippets in the documentation match the actual source.
- Confirm file paths referenced in the documentation are valid: `ls ./src/path/to/file.c`.
- If any discrepancies are found between MCP-sourced information and the local source tree, the local source tree is authoritative.

Input files:
- All component_*.md files from Stage 2
- All diagrams/*.mermaid files
- architecture_map.json for reference

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): MVCC's role in PostgreSQL's concurrency model, design philosophy, and key trade-offs
   - Architecture Overview: System-wide perspective with main structural diagram showing how components interact
   - Core Components (organized by lifecycle stage):
     a. Transaction Management — lifecycle from BEGIN to COMMIT/ROLLBACK
     b. Tuple Versioning — how rows are physically stored and updated
     c. Visibility Determination — how each transaction sees a consistent view
     d. Snapshot Management — acquiring and using snapshots at different isolation levels
     e. Concurrency Infrastructure — shared memory, locking, ProcArray
     f. CLOG and Status Persistence — durable transaction status tracking
     g. Garbage Collection — VACUUM, freezing, and space reclamation
   - Deep Dives: Complex topics including:
     - SSI (Serializable Snapshot Isolation) and rw-conflict detection
     - HOT update chains and index implications
     - Freeze map and visibility map optimization
     - Interaction between MVCC and WAL (crash recovery implications)
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of MVCC-related terms
     - Key data structure reference (HeapTupleHeaderData, SnapshotData, PGPROC, etc.)
     - Further reading (relevant PostgreSQL source files, commit messages, wiki pages)

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections
   - Eliminate redundancy while maintaining each section's standalone readability
   - Standardize terminology (prefer PostgreSQL official terms: e.g., "tuple" not "row" in implementation context, "xact" for transaction internals)
   - Add navigation aids (Table of Contents, section breadcrumbs, next/prev links)
   - Ensure consistent diagram style and labeling across all Mermaid diagrams

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in the output
   - Ensure logical flow: high-level concepts → architecture → implementation details
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Since total size will likely exceed 2000 lines:
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and "Next" notes per module
   - Each module should be self-contained enough for targeted reading

   Module structure:
   - index.md                          (navigation hub, reading guide)
   - 01_executive_summary.md           (overview for newcomers)
   - 02_architecture_overview.md       (system-wide perspective, main diagram)
   - 03_transaction_lifecycle.md       (BEGIN → COMMIT/ROLLBACK)
   - 04_tuple_versioning.md            (HeapTuple, version chains, HOT)
   - 05_visibility_rules.md            (HeapTupleSatisfiesMVCC, hint bits)
   - 06_snapshot_management.md         (snapshot types, isolation levels)
   - 07_concurrency_infrastructure.md  (ProcArray, locking, PGPROC, SSI)
   - 08_clog_transaction_status.md     (pg_xact, SLRU, status transitions)
   - 09_vacuum_and_freezing.md         (lazy vacuum, freeze, autovacuum)
   - 10_deep_dives.md                  (SSI, HOT chains, freeze map, MVCC+WAL)
   - appendix_symbol_index.md          (alphabetical symbol reference)
   - appendix_glossary.md              (MVCC terminology)
   - appendix_data_structures.md       (key struct definitions)

5. Additional Deliverables:
   - mvcc_quick_reference.md   (2-page summary: key concepts, critical functions, common debugging tips)
   - mvcc_api_reference.md     (function signatures grouped by subsystem, with brief descriptions)
   - quality_report.md         (coverage metrics: % of key_symbols documented, diagram count, known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. Execute each stage sequentially — do not proceed until the previous stage completes successfully
2. Capture all output files from each subagent
3. Validate expected outputs before proceeding to the next stage
4. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**. If MCP tool results conflict with the local source code, always prefer the local source.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions) and MCP tools for indexed cross-reference queries.
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Error Handling
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint
- **MCP server errors**: Implement exponential backoff (1s, 2s, 4s, max 3 retries) before failing gracefully
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `Pg` prefix), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical path symbols are documented (transaction commit, visibility check, vacuum, snapshot acquisition, tuple update)
2. Count and list all generated diagrams (must be ≥ 7)
3. Check total documentation coverage against key_symbols.txt (target > 80%)
4. Ensure no broken cross-references or unresolved TODO markers remain
5. Confirm file organization follows the specified module structure
6. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive MVCC documentation generated covering all 7 functional areas
- [ ] Minimum 7 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL MVCC Documentation Generation - Stage 1: Architecture Analysis"