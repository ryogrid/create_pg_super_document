# PostgreSQL Executor Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **Executor** subsystem, covering the complete lifecycle from plan tree reception through node initialization, tuple-at-a-time execution (Volcano/iterator model), expression evaluation, qualification and projection, join and aggregation strategies, parallel query execution, modification operations (INSERT/UPDATE/DELETE/MERGE), and executor shutdown. The documentation must include a **systematic catalog of all executor node types (operations)** with per-node implementation details, cost characteristics, and usage contexts.

## Output Directory
All generated artifacts — intermediate files (architecture_map.json, key_symbols.txt, etc.), component files, diagrams, and final documentation modules — **must** be written under the following directory:

```
topic_specific_generated_docs/about_executor/
```

Create this directory at the start of Stage 1 if it does not already exist. Use subdirectories as needed to organize outputs by stage:

```
topic_specific_generated_docs/about_executor/
├── stage1/                          # Architecture analysis outputs
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   └── initial_outline.md
├── stage2/                          # Detailed documentation components
│   ├── component_*.md
│   ├── node_catalog/                # Per-node-type documentation
│   │   ├── scan_nodes.md
│   │   ├── join_nodes.md
│   │   ├── sort_and_aggregate_nodes.md
│   │   ├── modifytable_node.md
│   │   ├── control_and_utility_nodes.md
│   │   └── parallel_nodes.md
│   └── diagrams/
│       └── *.mermaid
├── final/                           # Integrated final documentation
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 15_deep_dives.md
│   ├── appendix_*.md
│   ├── executor_quick_reference.md
│   ├── executor_api_reference.md
│   └── quality_report.md
└── diagrams/                        # Final consolidated diagrams
    └── *.mermaid
```

**All file paths referenced between stages (e.g., Stage 2 reading Stage 1 outputs) must use paths relative to `topic_specific_generated_docs/about_executor/`.**

## Available Resources

### MCP Server Capabilities
You have access to a specialized MCP server with these functions:
- `pg_symbol_overview(symbol)` - Get concise overview (low context usage)
- `pg_symbol_document(symbol)` - Get detailed documentation
- `pg_symbol_source(symbol)` - Retrieve source code for a symbol
- `pg_references_from(symbol)` - Get symbols referenced by this symbol
- `pg_references_to(symbol)` - Get symbols that reference this symbol

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for Executor documentation:

| Directory | Contents |
|---|---|
| `src/backend/executor/` | **Executor core** — `execMain.c` (ExecutorStart/Run/Finish/End), `execProcnode.c` (ExecInitNode/ExecProcNode/ExecEndNode dispatch), `execExpr.c` / `execExprInterp.c` (expression evaluation), `execQual.c` (qualification — may be merged into execExpr), `execTuples.c` (TupleTableSlot management), `execScan.c` (generic scan framework), `execJunk.c` (junk filter), `execUtils.c` (executor utilities), `execParallel.c` (parallel query), `execPartition.c` (partition routing), `execReplication.c` |
| `src/backend/executor/node*.c` | **Individual node implementations** — `nodeSeqscan.c`, `nodeIndexscan.c`, `nodeIndexonlyscan.c`, `nodeBitmapIndexscan.c`, `nodeBitmapHeapscan.c`, `nodeTidscan.c`, `nodeSubqueryscan.c`, `nodeFunctionscan.c`, `nodeValuesscan.c`, `nodeCtescan.c`, `nodeWorktablescan.c`, `nodeForeignscan.c`, `nodeCustom.c`, `nodeSamplescan.c`, `nodeNestloop.c`, `nodeMergejoin.c`, `nodeHashjoin.c`, `nodeHash.c`, `nodeMaterial.c`, `nodeSort.c`, `nodeIncrementalSort.c`, `nodeGroup.c`, `nodeAgg.c`, `nodeWindowAgg.c`, `nodeUnique.c`, `nodeSetOp.c`, `nodeLockRows.c`, `nodeLimit.c`, `nodeResult.c`, `nodeProjectSet.c`, `nodeModifyTable.c`, `nodeAppend.c`, `nodeMergeAppend.c`, `nodeRecursiveunion.c`, `nodeGather.c`, `nodeGatherMerge.c`, `nodeMemoize.c` |
| `src/backend/executor/` (SPI) | `spi.c` (Server Programming Interface — internal SQL execution from C code) |
| `src/include/executor/` | Key headers — `executor.h` (public executor API), `execExpr.h` (expression evaluation state), `execdesc.h` (QueryDesc), `execPartition.h`, `execParallel.h`, `nodeSeqscan.h`, `nodeIndexscan.h`, etc. (per-node headers), `spi.h`, `tuptable.h` (TupleTableSlot) |
| `src/include/nodes/` | **Node type definitions** — `plannodes.h` (Plan, SeqScan, IndexScan, NestLoop, HashJoin, ... — planner output), `execnodes.h` (PlanState, ScanState, JoinState, ... — executor runtime state), `primnodes.h` (Expr, Var, Const, OpExpr, ... — expression tree primitives), `pathnodes.h` (planner internal path nodes) |
| `src/backend/nodes/` | Node support — `nodeFuncs.c` (node tree walkers, mutators), `copyfuncs.c`, `equalfuncs.c`, `outfuncs.c`, `readfuncs.c` |
| `src/backend/access/heap/` | Heap access — `heapam.c` (heap_getnext, heap_insert, heap_update, heap_delete — called by executor scan/modify nodes) |
| `src/backend/access/index/` | Index access — `indexam.c` (index_getnext_slot — generic index scan interface), `genam.c` |
| `src/backend/optimizer/plan/` | Plan creation — `createplan.c` (creates Plan tree from Path tree — the executor's input), `planner.c`, `subselect.c` |
| `src/backend/tcop/` | Traffic cop — `pquery.c` (PortalRun → ExecutorRun bridge), `postgres.c` (top-level query processing loop) |
| `src/backend/utils/sort/` | Sort support — `tuplesort.c` (external sort used by Sort, MergeJoin, Aggregate nodes), `tuplestore.c` (tuple buffer for CTE, WindowAgg, Material) |
| `src/backend/utils/hash/` | Hash support — `dynahash.c` (dynamic hash tables used by HashJoin, Hash Aggregate) |
| `src/backend/utils/mmgr/` | Memory management — `aset.c` (AllocSet), `generation.c` (GenerationContext — per-tuple allocation) |
| `src/backend/jit/` | JIT compilation — `jit.c` (JIT framework), `llvmjit.c`, `llvmjit_expr.c` (LLVM-based expression compilation) |
| `src/include/utils/` | `tuplestore.h`, `tuplesort.h`, `memutils.h` (memory context hierarchy) |

**Usage guidelines for source code**:
- **Prefer direct source reading** over MCP `pg_symbol_source()` when exploring file-level structure, neighboring functions, or header definitions. Use `cat`, `grep`, `find`, and `head`/`tail` to navigate the tree.
- **Use MCP tools** for targeted symbol lookups, cross-reference analysis, and pre-indexed documentation.
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions that MCP may not fully index.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/executor/execMain.c:234`) for traceability.
- **For the node catalog**: use `ls src/backend/executor/node*.c` to enumerate all node implementation files, then read each file's Init/Exec/End functions systematically.

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Executor subsystem architecture.

Use BOTH the MCP server tools AND the local source tree (`./src/`) for analysis.

**Source exploration strategy for this stage**:
- Start by scanning key directories to identify relevant files:
  - `ls -la ./src/backend/executor/`
  - `ls ./src/backend/executor/node*.c | sort` (enumerate all node types)
  - `find ./src/include/nodes/ -name '*.h'`
  - `find ./src/include/executor/ -name '*.h'`
  - `find ./src/backend/utils/sort/ -name '*.c'`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols the MCP index might miss.
- Read key header files (`src/include/nodes/execnodes.h`, `src/include/nodes/plannodes.h`, `src/include/executor/executor.h`, `src/include/executor/tuptable.h`) to understand data structure definitions.
- Enumerate all executor node types by grepping: `grep -rn 'T_.*State' src/include/nodes/execnodes.h` and `grep -rn 'T_.*Scan\|T_.*Join\|T_Sort\|T_Agg' src/include/nodes/plannodes.h`
- Cross-validate MCP `pg_references_from()` / `pg_references_to()` results against `grep` results in the source tree.

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. Executor lifecycle and top-level control flow
   - Query processing pipeline: parse → analyze → rewrite → plan → **execute**
   - QueryDesc structure: snapshot, plan tree, destination, parameters
   - ExecutorStart (InitPlan, ExecInitNode tree walk) — plan tree → state tree construction
   - ExecutorRun (ExecProcNode on root node) — tuple-at-a-time pull model
   - ExecutorFinish (ExecEndNode tree walk, after-trigger processing)
   - ExecutorEnd — cleanup and resource release
   - Portal integration: PortalRunSelect → ExecutorRun bridge (pquery.c)
   - EXPLAIN ANALYZE instrumentation attachment (Instrumentation, InstrStartNode, InstrEndLoop)

2. Volcano/iterator execution model
   - Pull-based (demand-driven) tuple flow: parent calls ExecProcNode on child
   - ExecInitNode / ExecProcNode / ExecEndNode dispatch (execProcnode.c) — the central switch on NodeTag
   - Uniform TupleTableSlot interface: every node returns one slot per call, NULL signals end-of-scan
   - Recursive tree execution: how a plan tree is walked top-down (init), demand-driven (exec), bottom-up (end)
   - Rescan protocol: ExecReScan for parameterized nested loop inner, merge join restart, etc.

3. Plan node → PlanState mapping and node type taxonomy
   - Plan (plannodes.h) → PlanState (execnodes.h) correspondence
   - Node type tag dispatch (NodeTag enum in nodes.h)
   - **Complete enumeration of all executor node types** by category:
     a. Scan nodes: SeqScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan, TidScan, TidRangeScan, SubqueryScan, FunctionScan, ValuesScan, TableFuncScan, CteScan, NamedTuplestoreScan, WorkTableScan, ForeignScan, CustomScan, SampleScan
     b. Join nodes: NestLoop, MergeJoin, HashJoin
     c. Materialization / Sort nodes: Material, Sort, IncrementalSort, Memoize
     d. Aggregation / Grouping nodes: Group, Aggregate (plain, sorted, hashed, mixed), WindowAgg, SetOp, Unique
     e. Data modification nodes: ModifyTable (INSERT, UPDATE, DELETE, MERGE), LockRows
     f. Control / Utility nodes: Result, ProjectSet, Append, MergeAppend, RecursiveUnion, Limit
     g. Parallel execution nodes: Gather, GatherMerge
     h. Auxiliary nodes: Hash (build phase for HashJoin)
   - For each node type, identify: source file, Init/Exec/End function names, corresponding Plan and PlanState struct names

4. TupleTableSlot abstraction
   - TupleTableSlot structure (tuptable.h): virtual vs heap vs minimal vs buffer tuple slot types
   - TupleTableSlotOps (virtual method table): getsomeattrs, getsysattr, materialize, copy, etc.
   - Slot lifecycle: ExecInitResultTupleSlotTL, ExecStoreHeapTuple, ExecStoreMinimalTuple, ExecStoreVirtualTuple, ExecClearTuple
   - Datum/isnull arrays and deforming (slot_getattr, slot_getsomeattrs_int)
   - Slot interaction with buffer manager: BufferHeapTupleTableSlot holds buffer pin

5. Expression evaluation
   - Expr tree (primnodes.h): Var, Const, OpExpr, FuncExpr, BoolExpr, ScalarArrayOpExpr, SubPlan, CaseExpr, CoalesceExpr, etc.
   - ExprState and ExprEvalStep: compiled step-based evaluation (execExpr.c)
   - ExecInitExpr — expression tree → ExprState compilation
   - ExecEvalExpr (execExprInterp.c) — dispatch-based step interpreter
   - ExecQual — boolean qualification evaluation (filter rows)
   - ExecProject — projection: compute target list values into result slot
   - JIT expression compilation (jit.c, llvmjit_expr.c): when and how expressions are JIT-compiled for performance

6. Memory context management in executor
   - Per-query memory context (estate->es_query_cxt)
   - Per-tuple memory context (econtext->ecxt_per_tuple_memory) — reset per tuple to avoid leaks
   - Per-output memory context (econtext->ecxt_per_output_memory)
   - ResetExprContext — critical per-tuple cleanup
   - ExprContext: how expression evaluation accesses scan tuple, inner tuple, outer tuple
   - Memory context switches during expression evaluation and node execution

7. Scan node infrastructure
   - ExecScan (execScan.c): generic scan loop — fetch next tuple, apply qual, project
   - ScanState base: ss_currentRelation, ss_currentScanDesc, ss_ScanTupleSlot
   - Table AM abstraction: table_beginscan, table_scan_getnextslot (tableam.h / tableam.c)
   - Index AM abstraction: index_beginscan, index_getnext_slot (indexam.c)
   - Bitmap scan two-phase execution: BitmapIndexScan builds TIDBitmap, BitmapHeapScan fetches matching heap pages
   - Scan direction (ForwardScanDirection, BackwardScanDirection) and cursor support

8. Join node infrastructure
   - JoinState base: jointype, joinqual, joinstate
   - NestLoop: simple nested iteration, parameterized inner rescan (ExecReScan)
   - MergeJoin: merge-join state machine (EXEC_MJ_* states), requirement for sorted input
   - HashJoin: two-phase execution — build hash table (MultiExecHash), probe phase; batch handling for large builds (hybridHash)
   - Outer join handling: null-extension of unmatched tuples, jointype dispatch
   - Semi-join and anti-join optimization

9. Aggregation and grouping
   - AggState: transition functions, combine functions, final functions
   - Aggregation strategies: AGG_PLAIN (no grouping), AGG_SORTED (pre-sorted input), AGG_HASHED (hash-based), AGG_MIXED
   - Hash aggregation: hash table construction, memory management (hash_agg_check_limits), spill to disk
   - GroupAggregate: sorted-input advance logic
   - WindowAgg: window frame management, window function evaluation, run conditions
   - GROUPING SETS / CUBE / ROLLUP implementation via multiple aggregation phases

10. ModifyTable and data modification
    - ModifyTable node: dispatches INSERT/UPDATE/DELETE/MERGE
    - ExecInsert, ExecUpdate, ExecDelete, ExecMerge
    - Trigger execution: BEFORE/AFTER row and statement triggers, trigger transition tables
    - Partition routing: ExecFindPartition → tuple routing for partitioned tables
    - ON CONFLICT (UPSERT): ExecOnConflictUpdate, speculative insertion protocol
    - RETURNING clause: how modified tuples are returned to caller
    - Foreign table modification: FDW callbacks for remote DML
    - Cross-partition UPDATE: delete + insert under the hood

11. Parallel query execution
    - Parallel-aware and parallel-safe node classification
    - Gather / GatherMerge: collecting tuples from parallel workers
    - ExecParallelInitialize / ExecParallelCreateReaders / ExecParallelFinish
    - DSM (Dynamic Shared Memory) for worker communication
    - Shared state propagation: parallel bitmap heap scan shared iterator, parallel hash join shared hash table, parallel sort
    - Parallel worker execution entry point (ParallelQueryMain)
    - Partial aggregation: parallel-aware Aggregate splits into partial + finalize

12. Executor interaction with planner output
    - Plan tree structure: how the planner's output is consumed by executor
    - Parameterized plans (Param nodes): how NestLoop inner plans reference outer variables
    - InitPlan and SubPlan: how subqueries are executed (correlated and uncorrelated)
    - Plan-level targetList vs executor projection
    - Partition pruning at execution time (runtime partition pruning)

13. SPI (Server Programming Interface)
    - SPI_connect / SPI_finish — setup and teardown
    - SPI_execute, SPI_execute_plan — internal SQL execution
    - SPI_prepare — prepared statement handling
    - SPI context and memory management: SPI_palloc, SPI_freetuple
    - Used by PL/pgSQL, triggers, user-defined functions

Generate (all files under `topic_specific_generated_docs/about_executor/stage1/`):
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 40 symbols ranked by importance — larger set due to many node types)
- initial_outline.md with suggested documentation structure
- node_type_inventory.txt — complete enumeration of every executor node type with: NodeTag, Plan struct name, PlanState struct name, source file, Init/Exec/End function names
```

**Expected Output Check**: Verify architecture_map.json contains at least 80 symbols (larger than usual due to numerous node types) and identifies 6+ critical paths (e.g., executor startup path, tuple fetch path through scan node, join execution path, aggregation path, ModifyTable path, parallel gather path). Verify node_type_inventory.txt lists at least 30 distinct node types.

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for the PostgreSQL Executor subsystem.

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function implementation from `./src/` and annotate key logic steps.
- When documenting the executor lifecycle, read `src/backend/executor/execMain.c` end-to-end — this is the single most critical file for understanding executor startup, execution, and shutdown.
- When documenting the dispatch mechanism, read `src/backend/executor/execProcnode.c` for ExecInitNode/ExecProcNode/ExecEndNode switch dispatch.
- When documenting expression evaluation, read `src/backend/executor/execExpr.c` (ExecInitExpr compilation) and `src/backend/executor/execExprInterp.c` (step-based interpreter).
- When documenting TupleTableSlot, read `src/backend/executor/execTuples.c` and `src/include/executor/tuptable.h`.
- When documenting scan nodes, read `src/backend/executor/execScan.c` for the generic scan framework, then read each `nodeXxxscan.c` file.
- When documenting join nodes, read `src/backend/executor/nodeNestloop.c`, `src/backend/executor/nodeMergejoin.c`, `src/backend/executor/nodeHashjoin.c`, and `src/backend/executor/nodeHash.c`.
- When documenting aggregation, read `src/backend/executor/nodeAgg.c` — this is one of the largest and most complex node files.
- When documenting ModifyTable, read `src/backend/executor/nodeModifyTable.c` — covers INSERT/UPDATE/DELETE/MERGE.
- When documenting parallel query, read `src/backend/executor/execParallel.c`, `src/backend/executor/nodeGather.c`, `src/backend/executor/nodeGatherMerge.c`.
- **For the node catalog**: for EACH node type listed in node_type_inventory.txt, read the corresponding `node*.c` source file and document the Init, Exec, and End functions. Read the corresponding Plan struct (from plannodes.h) and PlanState struct (from execnodes.h).
- For data structure documentation, directly quote struct definitions from header files (e.g., `PlanState` from `src/include/nodes/execnodes.h`, `TupleTableSlot` from `src/include/executor/tuptable.h`).
- Include file paths and line numbers in all source references for traceability.
- Use `grep -rn` to find all callers of key functions to document integration patterns accurately.

Input files (from `topic_specific_generated_docs/about_executor/stage1/`):
- architecture_map.json
- key_symbols.txt
- initial_outline.md
- node_type_inventory.txt

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics and concurrency implications
   - Key invariants and assumptions

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader executor system
   - Key relationships to Tier 1 symbols

3. **Executor Node Catalog** (dedicated documentation for every node type):
   For EACH executor node type, produce a standardized entry containing:
   - **Identity**: NodeTag, Plan struct name, PlanState struct name, source file path
   - **Purpose**: what SQL constructs or query plan patterns produce this node
   - **Init function**: what resources are allocated, child nodes initialized, scan descriptors opened
   - **Exec function**: step-by-step execution logic for producing the next tuple
     - For scan nodes: how tuples are fetched, qualification applied, projection performed
     - For join nodes: how outer and inner tuple streams are combined, join condition checked
     - For aggregation nodes: how transition values are accumulated, finalized, and emitted
     - For sort/material nodes: how tuples are buffered and replayed
   - **End function**: cleanup, resource release
   - **Rescan behavior**: how ExecReScan resets the node for re-execution (parameterized rescans, etc.)
   - **State struct fields**: key PlanState subtype fields and their roles
   - **Performance characteristics**: time complexity, memory usage, I/O patterns
   - **Parallel support**: whether the node is parallel-aware or parallel-safe, and how it cooperates with Gather
   - **Planner integration**: under what conditions the planner chooses this node over alternatives
   - **Example**: a representative SQL query and its EXPLAIN output that exercises this node

4. Required Diagrams (minimum 10):
   - Executor lifecycle state diagram (ExecutorStart → ExecutorRun → ExecutorFinish → ExecutorEnd)
   - Volcano/iterator model: tuple flow through a multi-level plan tree (pull-based demand propagation)
   - ExecInitNode / ExecProcNode / ExecEndNode dispatch flowchart
   - Node type taxonomy tree (categorized by scan / join / aggregation / sort / modify / control / parallel)
   - TupleTableSlot type hierarchy and virtual method dispatch diagram
   - Expression evaluation pipeline (Expr tree → ExecInitExpr → ExprState steps → ExecEvalExpr interpreter)
   - HashJoin two-phase execution: build phase (MultiExecHash) → probe phase (ExecHashJoin)
   - MergeJoin state machine diagram (EXEC_MJ_* state transitions)
   - ModifyTable dispatch diagram (INSERT/UPDATE/DELETE/MERGE paths, trigger firing points, partition routing)
   - Parallel query architecture: Gather node, DSM, worker processes, partial aggregation flow

5. Special Focus Areas (dedicate extra depth):
   - Executor lifecycle: complete annotated walkthrough from QueryDesc creation through plan execution to cleanup, showing exactly which functions are called and in what order
   - Volcano model mechanics: how the pull-based model works with concrete examples showing tuple flow through SeqScan → HashJoin → Sort → Limit
   - ExecScan generic loop: the ExecScan → ExecScanFetch → ExecQual → ExecProject pipeline and how node-specific fetch functions integrate
   - HashJoin internals: bucket allocation, hash function selection, batch management for work_mem overflow, skew optimization, inner/outer join null-extension
   - Aggregate node complexity: the four strategies (plain/sorted/hashed/mixed), transition state management, GROUPING SETS phase enumeration, hash aggregate spill-to-disk
   - ModifyTable deep dive: trigger firing order (BEFORE STATEMENT → BEFORE ROW → actual operation → AFTER ROW → AFTER STATEMENT), RETURNING processing, ON CONFLICT paths, cross-partition updates
   - Expression evaluation compilation: how ExecInitExpr converts an Expr tree into a flat ExprEvalStep array, and how the step-based interpreter dispatches each step
   - JIT compilation: when JIT is triggered (jit_above_cost), what is JIT-compiled (expressions, tuple deforming, aggregation), LLVM integration
   - Parallel query coordination: how shared state is set up, how tuple queues work, how partial results are merged, interaction with buffer manager and locks
   - Memory management patterns: per-tuple context reset cycle, why it matters for correctness and memory consumption, SRF (set-returning function) memory context issues
   - Rescan protocol: complete rules for when and how each node type handles ExecReScan, interaction with chgParam and parameter changes

6. Source code references:
   - For each major function, include the relevant source file path
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values (e.g., NUM_EXEC_MJ_STATES, AGG_HASHED, EXEC_FLAG_REWIND, etc.)

Generate component files organized by functional area (all files under `topic_specific_generated_docs/about_executor/stage2/`):
- component_executor_lifecycle.md         (ExecutorStart/Run/Finish/End, QueryDesc, Portal integration)
- component_volcano_model.md              (iterator model, ExecProcNode dispatch, tuple flow, rescan)
- component_tuple_table_slot.md           (TupleTableSlot types, ops, lifecycle, deforming)
- component_expression_evaluation.md      (ExprState, ExecInitExpr, ExecEvalExpr, ExecQual, ExecProject, JIT)
- component_memory_management.md          (per-query/per-tuple/per-output contexts, ResetExprContext, ExprContext)
- component_scan_infrastructure.md        (ExecScan, ScanState, table AM, index AM, scan direction)
- component_join_infrastructure.md        (JoinState, NestLoop, MergeJoin, HashJoin, outer/semi/anti join)
- component_aggregation_grouping.md       (AggState, strategies, WindowAgg, GroupingSets, hash spill)
- component_modifytable.md                (INSERT/UPDATE/DELETE/MERGE, triggers, partition routing, ON CONFLICT, RETURNING)
- component_parallel_execution.md         (Gather, GatherMerge, DSM, worker coordination, partial aggregation)
- component_planner_interface.md          (Plan tree structure, Param, SubPlan/InitPlan, runtime pruning)
- component_spi.md                        (SPI_connect, SPI_execute, SPI_prepare, memory management)
- node_catalog/scan_nodes.md              (SeqScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan, TidScan, TidRangeScan, SubqueryScan, FunctionScan, ValuesScan, TableFuncScan, CteScan, NamedTuplestoreScan, WorkTableScan, ForeignScan, CustomScan, SampleScan)
- node_catalog/join_nodes.md              (NestLoop, MergeJoin, HashJoin — full implementation details)
- node_catalog/sort_and_aggregate_nodes.md (Sort, IncrementalSort, Material, Memoize, Group, Aggregate, WindowAgg, Unique, SetOp)
- node_catalog/modifytable_node.md        (ModifyTable, LockRows — full implementation details)
- node_catalog/control_and_utility_nodes.md (Result, ProjectSet, Append, MergeAppend, RecursiveUnion, Limit)
- node_catalog/parallel_nodes.md          (Gather, GatherMerge, Hash — parallel-aware behavior)
- diagrams/*.mermaid                      (under `topic_specific_generated_docs/about_executor/stage2/diagrams/`)
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 10 diagrams are generated. Verify every node type from node_type_inventory.txt has a catalog entry in the node_catalog/ files.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical document.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 15 critical function signatures and struct definitions against `./src/` to ensure accuracy (more than usual due to the large number of node types).
- Verify that all quoted code snippets in the documentation match the actual source.
- Confirm file paths referenced in the documentation are valid: `ls ./src/path/to/file.c`.
- If any discrepancies are found between MCP-sourced information and the local source tree, the local source tree is authoritative.
- Cross-check every node_catalog entry: verify the Init/Exec/End function names match the actual function names in the source file.

Input files (from `topic_specific_generated_docs/about_executor/stage2/`):
- All component_*.md files from Stage 2
- All node_catalog/*.md files from Stage 2
- All diagrams/*.mermaid files
- architecture_map.json for reference (from `topic_specific_generated_docs/about_executor/stage1/`)
- node_type_inventory.txt for reference (from `topic_specific_generated_docs/about_executor/stage1/`)

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): The executor's role in PostgreSQL's query processing pipeline, the Volcano/iterator design philosophy, and key architectural trade-offs (pull model simplicity vs. push model efficiency, tuple-at-a-time vs. vectorized)
   - Architecture Overview: System-wide perspective with main structural diagram showing the executor's position between planner and storage layer, and how the plan tree drives execution
   - Core Components (organized by operational flow):
     a. Executor Lifecycle — ExecutorStart/Run/Finish/End, QueryDesc, Portal integration, EXPLAIN ANALYZE instrumentation
     b. Volcano/Iterator Model — pull-based tuple flow, ExecProcNode dispatch, rescan protocol
     c. TupleTableSlot Abstraction — slot types, virtual method dispatch, slot lifecycle
     d. Expression Evaluation — ExprState compilation, step-based interpreter, ExecQual, ExecProject, JIT compilation
     e. Memory Context Management — per-query, per-tuple, per-output contexts, ExprContext, ResetExprContext
     f. Scan Node Infrastructure — ExecScan generic loop, Table AM / Index AM integration, scan direction
     g. Join Node Infrastructure — NestLoop, MergeJoin, HashJoin, outer/semi/anti join handling
     h. Aggregation and Grouping — AggState, hash vs sorted aggregation, WindowAgg, GROUPING SETS
     i. ModifyTable and Data Modification — INSERT/UPDATE/DELETE/MERGE, triggers, partitioning, RETURNING
     j. Parallel Query Execution — Gather, workers, DSM, partial aggregation, parallel-aware nodes
     k. Planner Interface — Plan tree consumption, Param, SubPlan, runtime partition pruning
     l. SPI — internal SQL execution interface for procedural code and triggers
   - **Node Catalog** (dedicated chapter):
     A comprehensive catalog of every executor node type, organized by category. Each entry follows a standardized template (identity, purpose, Init/Exec/End logic, state fields, performance, parallel support, example SQL). This chapter serves as both reference documentation and a learning guide for understanding PostgreSQL's execution strategies.
     - Scan Nodes (with sub-entries for each type)
     - Join Nodes (with sub-entries for each type)
     - Sort and Materialization Nodes
     - Aggregation and Grouping Nodes
     - Data Modification Nodes
     - Control and Utility Nodes
     - Parallel Execution Nodes
   - Deep Dives: Complex topics including:
     - HashJoin batch management and work_mem overflow handling
     - Aggregate node's four-strategy dispatch and hash spill-to-disk
     - MergeJoin state machine: complete state transition analysis
     - Parallel query shared state: shared hash tables, shared sort, shared bitmap
     - JIT compilation pipeline: when triggered, what is compiled, LLVM IR generation
     - Trigger execution ordering and transition table management
     - Executor hooks and extensibility (ExecutorStart_hook, ExecutorRun_hook, etc.)
     - Interaction between executor and buffer manager: pin/unpin patterns during scan
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of executor terms
     - Key data structure reference (PlanState, TupleTableSlot, ExprState, AggState, HashJoinState, etc.)
     - Node type quick-reference table (NodeTag → Plan struct → PlanState struct → source file → Init/Exec/End functions — one row per node type)
     - Further reading (relevant PostgreSQL source files, README files in source tree, wiki pages, executor-related commit messages)

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections (e.g., scan node catalog entries link back to the scan infrastructure chapter, and vice versa)
   - Eliminate redundancy between the component chapters and the node catalog — the catalog should focus on per-node specifics while the chapters provide cross-cutting concepts
   - Standardize terminology (prefer PostgreSQL implementation terms: e.g., "plan node" vs "operator", "tuple" in implementation context, "slot" for TupleTableSlot, "qual" for qualification)
   - Add navigation aids (Table of Contents, section breadcrumbs, next/prev links)
   - Ensure consistent diagram style and labeling across all Mermaid diagrams
   - For the node catalog: ensure every entry has a "Example SQL" subsection showing a query that exercises the node (with representative EXPLAIN output)

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in the output
   - Verify all node types from node_type_inventory.txt have catalog entries
   - Ensure logical flow: high-level concepts → architecture → implementation details → node-by-node reference
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Since total size will likely exceed 3000 lines (larger than usual due to the node catalog):
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and "Next" notes per module
   - Each module should be self-contained enough for targeted reading
   - **All final output files must be written under `topic_specific_generated_docs/about_executor/final/`**
   - **Consolidated diagrams must be copied to `topic_specific_generated_docs/about_executor/diagrams/`**

   Module structure (all under `topic_specific_generated_docs/about_executor/final/`):
   - index.md                               (navigation hub, reading guide)
   - 01_executive_summary.md                (overview for newcomers)
   - 02_architecture_overview.md            (system-wide perspective, main diagram)
   - 03_executor_lifecycle.md               (ExecutorStart/Run/Finish/End, QueryDesc, Portal)
   - 04_volcano_iterator_model.md           (pull-based execution, dispatch, rescan)
   - 05_tuple_table_slot.md                 (slot types, ops, lifecycle, deforming)
   - 06_expression_evaluation.md            (ExprState, interpreter, ExecQual, ExecProject, JIT)
   - 07_memory_context_management.md        (per-query/per-tuple contexts, ExprContext)
   - 08_scan_infrastructure.md              (ExecScan, Table AM, Index AM)
   - 09_join_infrastructure.md              (NestLoop, MergeJoin, HashJoin)
   - 10_aggregation_and_grouping.md         (AggState, strategies, WindowAgg, GroupingSets)
   - 11_modifytable.md                      (INSERT/UPDATE/DELETE/MERGE, triggers, partition routing)
   - 12_parallel_execution.md              (Gather, workers, DSM, partial aggregation)
   - 13_planner_interface.md                (Plan tree, Param, SubPlan, runtime pruning)
   - 14_spi.md                              (Server Programming Interface)
   - 15_node_catalog_scan.md                (all scan node types — detailed catalog)
   - 16_node_catalog_join.md                (all join node types — detailed catalog)
   - 17_node_catalog_sort_aggregate.md      (sort, materialization, aggregation, grouping nodes)
   - 18_node_catalog_modify_control.md      (ModifyTable, LockRows, Result, Append, Limit, etc.)
   - 19_node_catalog_parallel.md            (Gather, GatherMerge, Hash — parallel-aware catalog)
   - 20_deep_dives.md                       (HashJoin batching, Agg spill, MergeJoin FSM, JIT, hooks)
   - appendix_symbol_index.md              (alphabetical symbol reference)
   - appendix_glossary.md                  (executor terminology)
   - appendix_data_structures.md           (key struct definitions)
   - appendix_node_quick_reference.md      (node type → struct → source → functions lookup table)

5. Additional Deliverables (also under `topic_specific_generated_docs/about_executor/final/`):
   - executor_quick_reference.md   (2-page summary: key concepts, critical functions, common debugging tips including EXPLAIN ANALYZE interpretation)
   - executor_api_reference.md     (function signatures grouped by subsystem, with brief descriptions)
   - quality_report.md             (coverage metrics: % of key_symbols documented, % of node types cataloged, diagram count, known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), complete node type catalog coverage (100% of node_type_inventory.txt entries), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. **Before Stage 1**: Create the output directory tree:
   ```bash
   mkdir -p topic_specific_generated_docs/about_executor/{stage1,stage2/diagrams,stage2/node_catalog,final,diagrams}
   ```
2. Execute each stage sequentially — do not proceed until the previous stage completes successfully
3. Capture all output files from each subagent into the appropriate subdirectory under `topic_specific_generated_docs/about_executor/`
4. Validate expected outputs before proceeding to the next stage
5. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**. If MCP tool results conflict with the local source code, always prefer the local source.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions) and MCP tools for indexed cross-reference queries.
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Error Handling
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint. **For the node catalog**: if context limits are hit, process node types in batches (scan nodes first, then join nodes, etc.)
- **MCP server errors**: Implement exponential backoff (1s, 2s, 4s, max 3 retries) before failing gracefully
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `Exec` prefix, `node` prefix), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %, node types cataloged>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical path symbols are documented (ExecutorStart, ExecutorRun, ExecInitNode, ExecProcNode, ExecEndNode, ExecScan, ExecEvalExpr, ExecInitExpr, ExecQual, ExecProject)
2. Verify all node type Init/Exec/End functions are documented in the node catalog
3. Count and list all generated diagrams (must be ≥ 10)
4. Check total documentation coverage against key_symbols.txt (target > 80%)
5. Check node catalog coverage against node_type_inventory.txt (target = 100%)
6. Ensure no broken cross-references or unresolved TODO markers remain
7. Confirm file organization follows the specified module structure
8. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive executor documentation generated covering all 13 functional areas
- [ ] Complete node catalog covering 100% of executor node types with standardized entries
- [ ] Minimum 10 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage and 100% node type catalog coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreS
QL Executor Documentation Generation - Stage 1: Architecture Analysis"