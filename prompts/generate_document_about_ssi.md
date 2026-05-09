# PostgreSQL SSI (Serializable Snapshot Isolation) Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's implementation of
Serializable isolation using SSI (Serializable Snapshot Isolation), covering the
entire lifecycle from transaction start and serializable snapshot acquisition,
through SIREAD predicate lock acquisition and granularity promotion,
rw-conflict detection and dangerous-structure tracking, commit-time validation,
abort/retry behavior, cleanup/summarization, memory pressure handling, and
integration with MVCC snapshots, lock manager internals, and transaction
management.

The documentation must explain both the algorithmic model and the concrete code
paths in PostgreSQL, including:
- Predicate lock target taxonomy (relation/page/tuple/index-range proxy targets)
- SerializableXact state machine and conflict graph representation
- rw-conflict-in / rw-conflict-out edge creation and dangerous structure checks
- Commit sequencing and pivot handling
- Read-only deferrable transaction optimization (safe snapshots)
- Interaction with subtransactions, two-phase commit, and recovery boundaries
- Error surfaces (serialization failures) and observability tooling

The documentation must also include:
- A systematic catalog of SSI core data structures
- A systematic catalog of SSI critical functions and their caller/callee mapping
- A focused catalog of predicate-lock APIs and conflict-check APIs

## Output Directory
All generated artifacts must be written under:

topic_specific_generated_docs/about_ssi/

Create this directory tree at the start of Stage 1 if missing:

topic_specific_generated_docs/about_ssi/
├── stage1/
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   ├── initial_outline.md
│   ├── ssi_data_structure_inventory.txt
│   ├── predicate_lock_api_inventory.txt
│   └── conflict_flow_inventory.txt
├── stage2/
│   ├── component_*.md
│   ├── catalogs/
│   │   ├── data_structures.md
│   │   ├── predicate_lock_apis.md
│   │   ├── conflict_detection_apis.md
│   │   ├── commit_validation_apis.md
│   │   └── monitoring_and_views.md
│   └── diagrams/
│       └── *.mermaid
├── final/
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 18_deep_dives.md
│   ├── appendix_*.md
│   ├── ssi_quick_reference.md
│   ├── ssi_api_reference.md
│   └── quality_report.md
└── diagrams/
		└── *.mermaid

All cross-stage references must use paths relative to
topic_specific_generated_docs/about_ssi/.

## Available Resources

### Local Source Code (PostgreSQL src/ directory)
The PostgreSQL source tree is available locally at ./src/. Use it as the single
source of truth.

Primary SSI-related locations (verify exact symbols in local source):

| Directory / File | Focus |
|---|---|
| src/backend/storage/lmgr/predicate.c | Core SSI and predicate-lock implementation (SIREAD locks, rw-conflicts, dangerous structures, commit checks, cleanup) |
| src/include/storage/predicate.h | Public predicate-lock/SSI interfaces |
| src/backend/storage/lmgr/README-SSI | Core conceptual documentation of PostgreSQL SSI design |
| src/backend/access/transam/xact.c | Transaction lifecycle hooks that invoke SSI checks/finalization |
| src/backend/utils/time/snapmgr.c | Snapshot handling and serializable snapshot flows |
| src/backend/storage/lmgr/proc.c | PGPROC / backend-process state interactions |
| src/backend/storage/ipc/procarray.c | Transaction visibility and process-array interactions |
| src/include/storage/proc.h | PGPROC fields relevant to SSI/predicate locking |
| src/include/utils/snapshot.h | Snapshot data model used by SSI checks |
| src/backend/storage/lmgr/lmgr.c and lock.c | Lock manager integration points and lock-tag behavior |
| src/backend/utils/adt/lockfuncs.c | SQL-visible lock inspection functions (cross-check with predicate locks visibility paths) |
| src/backend/catalog/system_views.sql | View definitions including lock/stat views tied to observability |
| src/doc/src/sgml/mvcc.sgml | User-facing serializable isolation behavior and caveats |

Usage guidelines:
- Read src/backend/storage/lmgr/README-SSI end-to-end first.
- Validate every function signature against source before documenting.
- Use grep -rn for call chains, state transitions, and lock target handling.
- Include source file path and line references in docs for traceability.
- If expected symbols differ by PostgreSQL version, record both observed and
	expected names in notes.

### Available Subagents
1. architecture-analyzer - Analyze SSI architecture and dependency graph
2. detail-documenter - Generate detailed SSI documentation components
3. integration-optimizer - Integrate modules and run coverage checks

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke architecture-analyzer with this instruction:

Analyze the PostgreSQL SSI (Serializable Snapshot Isolation) subsystem
architecture and implementation.

Use local source tree (./src/) as ground truth.

Source exploration strategy:
- Read src/backend/storage/lmgr/README-SSI completely first.
- Read src/backend/storage/lmgr/predicate.c end-to-end.
- Read src/include/storage/predicate.h end-to-end.
- Scan related implementation files:
	- find ./src/backend/storage/lmgr/ -name '*.c'
	- find ./src/backend/access/transam/ -name '*.c'
	- find ./src/backend/utils/time/ -name '*.c'
	- find ./src/backend/storage/ipc/ -name '*.c'
	- find ./src/include/storage/ -name '*.h'
	- find ./src/include/utils/ -name '*.h'
- Trace key symbol call chains with grep -rn.

Build a dependency map (depth 5) with focus on:

1. Transaction lifecycle integration
	 - Entry points from transaction start to commit/abort for serializable mode
	 - Snapshot acquisition and serializable transaction registration
	 - Pre-commit validation path and failure path
	 - Post-commit and post-abort cleanup paths

2. Serializable transaction state model
	 - Core structures representing serializable transactions and conflict edges
	 - State flags and transitions (active, prepared, committed, doomed, summarized)
	 - Commit sequence number / ordering metadata if present

3. Predicate lock model
	 - Lock targets and granularity levels (relation/page/tuple and index-range proxies)
	 - Acquisition APIs and lock promotion/coalescing behavior
	 - Per-transaction lock ownership tracking and transfer rules
	 - Memory bounds and summarization/cleanup strategy

4. rw-conflict graph mechanics
	 - Conflict-in and conflict-out edge insertion
	 - Dangerous-structure detection logic
	 - Read-only optimization logic and safe snapshot checks
	 - Pivot handling and false-positive minimization strategy

5. Commit-time serialization checks
	 - Validation routine(s) before commit durability point
	 - How serialization failures are raised
	 - How retry semantics are surfaced to callers/users

6. Subtransactions and 2PC
	 - Subtransaction state propagation to parent
	 - Prepared transaction behavior and persistent metadata interactions
	 - Cleanup semantics on rollback to savepoint and full abort

7. Concurrency internals and synchronization
	 - LWLock / spinlock protected structures used by SSI
	 - Shared-memory hash tables and freelists used for conflict/predicate state
	 - ProcArray / PGPROC integration

8. Observability and tooling
	 - SQL-visible lock views and functions (including predicate lock visibility)
	 - Logging/error messages for serialization failures
	 - Any debug or trace hooks/macros

Generate under topic_specific_generated_docs/about_ssi/stage1/:
- architecture_map.json:
	- at least 100 symbols with importance score 0.0-1.0
	- include files, callers, callees, and subsystem tag
- key_symbols.txt:
	- top 60 symbols ranked by importance
- initial_outline.md:
	- proposed module structure for Stage 2 and Stage 3
- ssi_data_structure_inventory.txt:
	- every key SSI struct (name, file:line, role, key fields)
- predicate_lock_api_inventory.txt:
	- every predicate-lock API (signature, role, called from)
- conflict_flow_inventory.txt:
	- ordered steps for read/write conflict registration and commit-time validation

Expected output checks:
- architecture_map.json has >=100 symbols and >=8 critical flows
	(snapshot path, lock-acquire path, conflict-edge path, dangerous-structure path,
	 pre-commit validation path, cleanup path, read-only deferrable path,
	 observability path)
- ssi_data_structure_inventory.txt has >=20 entries
- predicate_lock_api_inventory.txt has >=20 API entries

### Stage 2: Detailed Documentation Generation
After Stage 1, invoke detail-documenter:

Using Stage 1 outputs, produce deep SSI technical documentation.

Mandatory source reading and validation:
- src/backend/storage/lmgr/README-SSI (conceptual baseline)
- src/backend/storage/lmgr/predicate.c (core implementation)
- src/include/storage/predicate.h (public interfaces)
- src/backend/access/transam/xact.c (lifecycle integration)
- src/backend/utils/time/snapmgr.c (snapshot interaction)
- src/backend/storage/ipc/procarray.c and src/backend/storage/lmgr/proc.c
	(shared process state)
- Any additional files discovered through call-chain traversal

Requirements:

1. Tiered symbol docs
- For symbols with importance >0.8:
	- Full API docs (signature, params, returns)
	- Internal logic walkthrough
	- Caller/callee map
	- Concurrency and correctness invariants
	- Complexity and contention notes
- For symbols with importance 0.5-0.8:
	- API summary and subsystem role
	- Key integration references

2. Data-structure catalog
For each SSI structure include:
- Identity (struct name, file:line)
- Role in SSI algorithm
- Key fields and invariants
- Lifetime (allocation, mutation, cleanup)
- Synchronization constraints

3. Predicate-lock API catalog
For each API include:
- Signature and usage context
- Lock target semantics
- Promotion/coalescing behavior
- Interaction with conflict detection
- Error and edge cases

4. Conflict-detection and commit-validation catalog
For each function include:
- When called in lifecycle
- Inputs and required preconditions
- Graph updates performed
- Dangerous structure checks
- Abort/serialization-failure trigger conditions

5. Required diagrams (minimum 12)
- End-to-end SSI lifecycle: begin -> snapshot -> read/write tracking -> commit check
- Predicate lock hierarchy and promotion flow
- rw-conflict graph (in/out edges) and dangerous structure motif
- Commit-time validation decision tree
- Read-only deferrable safe-snapshot path
- Subtransaction and savepoint behavior in SSI metadata
- 2PC path with serializable transactions
- Shared-memory objects and lock protection boundaries
- SQL observability surfaces (predicate lock inspection flow)
- Cleanup/summarization lifecycle across commit and abort
- Interaction between MVCC visibility and SSI conflict checks
- Serialization failure propagation to client

6. Special deep-focus topics
- Why SSI is used instead of strict 2PL in PostgreSQL
- SIREAD lock semantics vs blocking locks
- False positives vs performance trade-offs
- Read-only optimization and safe snapshots
- Memory pressure handling and summarization
- Commit ordering implications and pivot transactions
- Interplay with vacuum/pruning/index access paths where relevant

Generate under topic_specific_generated_docs/about_ssi/stage2/:
- component_lifecycle_and_entry_points.md
- component_snapshot_and_registration.md
- component_predicate_locking.md
- component_conflict_graph_and_detection.md
- component_commit_validation_and_abort_paths.md
- component_subtransactions_and_2pc.md
- component_concurrency_and_shared_memory.md
- component_observability_and_debugging.md
- component_performance_and_tuning.md
- component_error_modes_and_retries.md
- component_hooks_and_extensibility.md
- catalogs/data_structures.md
- catalogs/predicate_lock_apis.md
- catalogs/conflict_detection_apis.md
- catalogs/commit_validation_apis.md
- catalogs/monitoring_and_views.md
- diagrams/*.mermaid

Expected output checks:
- All Tier 1 symbols documented with source references
- >=12 diagrams generated
- Every Stage 1 predicate-lock API appears in catalogs/predicate_lock_apis.md
- Every Stage 1 conflict/commit function appears in relevant catalog

### Stage 3: Integration and Optimization
After Stage 2, invoke integration-optimizer:

Integrate all SSI documentation into a coherent final technical manual.

Verification requirements before finalization:
- Spot-check >=20 function signatures and >=10 struct definitions against source
- Validate all quoted code snippets against local files
- Validate file paths exist using ls for each referenced source path set
- Validate all Mermaid files have syntactically valid diagrams
- Cross-check symbol coverage against key_symbols.txt

Integration requirements:

1. Document structure in final/
- index.md
- 01_executive_summary.md
- 02_architecture_overview.md
- 03_lifecycle_and_entry_points.md
- 04_snapshot_and_registration.md
- 05_predicate_locking.md
- 06_conflict_graph_and_detection.md
- 07_commit_validation_and_abort_paths.md
- 08_subtransactions_and_2pc.md
- 09_concurrency_and_shared_memory.md
- 10_observability_and_debugging.md
- 11_performance_and_tuning.md
- 12_error_modes_and_retries.md
- 13_hooks_and_extensibility.md
- 14_catalog_data_structures.md
- 15_catalog_predicate_lock_apis.md
- 16_catalog_conflict_and_commit_apis.md
- 17_case_studies.md
- 18_deep_dives.md
- appendix_symbol_index.md
- appendix_glossary.md
- appendix_source_map.md
- appendix_invariants_checklist.md
- appendix_configuration_notes.md
- ssi_quick_reference.md
- ssi_api_reference.md
- quality_report.md

2. Deep dives (must include)
- Dangerous structure detection internals
- Read-only deferrable transaction safe-snapshot algorithm
- Serialization failure case studies and replay timelines
- Predicate lock granularity trade-offs and promotion heuristics
- Shared-memory scalability and contention hotspots

3. Navigation and consistency
- Table of contents with prerequisites/next links
- Consistent terminology and naming
- Cross-links between conceptual chapters and API catalogs
- Duplicate-content reduction between chapters and catalogs

4. Quality report metrics
- Symbol coverage % (target >80%)
- Data-structure catalog coverage % (target 100% of Stage 1 inventory)
- Predicate-lock API coverage % (target 100% of Stage 1 inventory)
- Conflict/commit API coverage % (target 100% of Stage 1 inventory)
- Diagram count (target >=12)
- Known gaps and follow-up recommendations

Also copy consolidated diagrams to:
topic_specific_generated_docs/about_ssi/diagrams/

Expected output checks:
- Professional quality and coherent flow
- >80% symbol coverage
- 100% catalog coverage against Stage 1 inventories
- >=12 valid diagrams

---

## Orchestration Rules

### Execution Flow
1. Execute stages sequentially
2. Validate expected outputs before moving to next stage
3. Retry failed subagent once with narrower scope, then continue with gaps noted
4. Emit progress report after each stage

### Source Primacy
- Local ./src/ is authoritative
- src/backend/storage/lmgr/README-SSI is mandatory reading baseline
- All major claims require source path references

### Error Handling
- On missing symbols: try alternate names and log unresolved items
- On context limits: split by subsystem and continue with checkpoints
- On partial outputs: proceed with explicit quality_report.md gaps

### Progress Reporting Format
After each stage, report:

[Stage X Complete]
Generated files: <list>
Key metrics: <symbols, diagrams, coverage values>
Issues encountered: <warnings or failures>
Next stage: <description>

### Final Validation Checklist
Before declaring completion verify:
1. Critical symbols documented, including representative lifecycle/conflict APIs
2. Stage 1 data structures covered 100% in final catalogs
3. Stage 1 predicate lock APIs covered 100% in final catalogs
4. Stage 1 conflict-flow APIs covered 100% in final catalogs
5. Diagram count >=12 and Mermaid syntax valid
6. Internal links resolve and no unresolved TODO markers remain
7. File organization matches final module list

### Success Criteria
Task is complete when all are true:
- All 3 stages executed
- Full SSI implementation flow documented from begin to cleanup
- Complete catalogs for data structures and SSI APIs
- >=12 technical diagrams included and valid
- quality_report.md shows >80% symbol coverage and 100% catalog coverage
- Documentation is navigable for both newcomers and contributors

---

## Start Execution
Begin Stage 1 immediately. Do not wait for confirmation between stages.
Proceed automatically if stage checks pass.

Report:
[Starting] PostgreSQL SSI Documentation Generation - Stage 1: Architecture Analysis
