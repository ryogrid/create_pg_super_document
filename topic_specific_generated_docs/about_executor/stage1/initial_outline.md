# PostgreSQL Executor Subsystem -- Documentation Outline

## Overview

The PostgreSQL Executor is the component that takes a query plan produced by the
planner/optimizer and actually executes it, producing result tuples.  It
implements the Volcano/iterator execution model: every plan node exposes the same
`ExecProcNode` interface, returning one tuple at a time in a demand-driven
(pull-based) fashion.

This document covers 13 functional areas spanning approximately 120 symbols
across 70+ source files in `src/backend/executor/` and supporting headers in
`src/include/executor/` and `src/include/nodes/`.

---

## Part I -- Executor Lifecycle and Top-Level Control Flow

**Estimated size: 6,000 words | Depth: Detailed**

### 1.1 Query Processing Pipeline Context
- Where the executor sits: parse -> analyze -> rewrite -> plan -> **execute**
- Entry from `ProcessQuery()` (tcop/pquery.c) and `PortalRunSelect()`
- The `QueryDesc` bridge structure: `PlannedStmt`, `Snapshot`, `DestReceiver`, `ParamListInfo`

### 1.2 ExecutorStart -- Plan State Tree Construction
- `ExecutorStart()` hook mechanism and `standard_ExecutorStart()`
- `CreateExecutorState()`: EState creation with per-query memory context
- Snapshot registration, command ID setup, trigger context initialization
- `InitPlan()`: the central initialization routine
  - Range table setup via `ExecInitRangeTable()`
  - Result relation initialization via `InitResultRelInfo()`
  - Recursive plan state tree construction via `ExecInitNode()`
  - Permission checking via `ExecCheckPermissions()`

### 1.3 ExecutorRun -- Tuple Retrieval Loop
- `ExecutorRun()` hook mechanism and `standard_ExecutorRun()`
- `ExecutePlan()`: the per-query execution loop
  - Calls `ExecProcNode()` on root node repeatedly
  - `ResetPerTupleExprContext()` per-tuple cleanup
  - Count-based termination and scan direction handling
  - Destination receiver protocol: `rStartup`, `receiveSlot`, `rShutdown`

### 1.4 ExecutorFinish -- After-Trigger Processing
- `ExecutorFinish()` hook and `standard_ExecutorFinish()`
- `ExecPostprocessPlan()`: SubPlan result processing
- `AfterTriggerEndQuery()`: deferred trigger execution

### 1.5 ExecutorEnd -- Cleanup and Resource Release
- `ExecutorEnd()` hook and `standard_ExecutorEnd()`
- `ExecEndPlan()`: recursive node cleanup
- `FreeExecutorState()`: memory context destruction
- JIT cleanup, snapshot unregistration

### 1.6 Portal Integration
- `PortalRunSelect()` (pquery.c): cursor/portal to ExecutorRun bridge
- Forward vs backward scan support
- Held-store (materialized) portal mode

### 1.7 EXPLAIN ANALYZE Instrumentation
- `EXEC_FLAG_EXPLAIN_ONLY` flag
- `InstrAlloc()`: per-node instrumentation structure allocation
- `InstrStartNode()` / `InstrStopNode()`: timing and buffer usage capture
- `ExecProcNodeInstr()` wrapper insertion
- `Instrumentation` structure: ntuples, startup/total time, buffer usage

---

## Part II -- Volcano/Iterator Execution Model

**Estimated size: 4,000 words | Depth: Detailed**

### 2.1 Pull-Based Tuple Flow
- Demand-driven model: parent calls `ExecProcNode()` on child
- Each call returns one `TupleTableSlot` or NULL (end of scan)
- Recursive tree execution: top-down init, demand-driven exec, bottom-up end

### 2.2 ExecInitNode / ExecProcNode / ExecEndNode Dispatch
- `execProcnode.c`: the central switch on `NodeTag`
- `ExecInitNode()`: Plan tree -> PlanState tree construction (40+ node types)
- `ExecProcNode()`: inline function dispatching via `ExecProcNodeMtd` function pointer
- `ExecSetExecProcNode()`: wrapper installation for stack checking and instrumentation
- `ExecProcNodeFirst()` / `ExecProcNodeInstr()`: first-call and instrumented wrappers
- `ExecEndNode()`: recursive cleanup dispatch

### 2.3 MultiExecProcNode -- Non-Tuple Returns
- For nodes returning complex structures: hash tables, bitmaps
- `MultiExecHash()`, `MultiExecBitmapIndexScan()`, `MultiExecBitmapAnd/Or()`

### 2.4 Rescan Protocol
- `ExecReScan()`: parameter change detection and dispatch
- `chgParam` bitmapset for tracking changed parameters
- Use cases: nested loop inner rescan, merge join restart, subquery re-evaluation

---

## Part III -- Plan Node to PlanState Mapping and Node Type Taxonomy

**Estimated size: 5,000 words | Depth: Reference/Catalog**

### 3.1 Plan / PlanState Correspondence
- `Plan` (plannodes.h) base fields: targetlist, qual, lefttree, righttree
- `PlanState` (execnodes.h) base fields: plan, state, ExecProcNode, qual, subtrees, projection
- `NodeTag` dispatch mechanism

### 3.2 Complete Node Type Enumeration (43 types)
*Reference to node_type_inventory.txt for complete details*

#### 3.2.1 Scan Nodes (17 types)
SeqScan, SampleScan, IndexScan, IndexOnlyScan, BitmapIndexScan, BitmapHeapScan,
TidScan, TidRangeScan, SubqueryScan, FunctionScan, TableFuncScan, ValuesScan,
CteScan, NamedTuplestoreScan, WorkTableScan, ForeignScan, CustomScan

#### 3.2.2 Join Nodes (3 types)
NestLoop, MergeJoin, HashJoin

#### 3.2.3 Materialization / Sort Nodes (4 types)
Material, Sort, IncrementalSort, Memoize

#### 3.2.4 Aggregation / Grouping Nodes (5 types)
Group, Agg, WindowAgg, Unique, SetOp

#### 3.2.5 Data Modification Nodes (2 types)
ModifyTable, LockRows

#### 3.2.6 Control / Utility Nodes (6 types)
Result, ProjectSet, Append, MergeAppend, RecursiveUnion, Limit

#### 3.2.7 Parallel Execution Nodes (2 types)
Gather, GatherMerge

#### 3.2.8 Auxiliary Nodes (2 types)
Hash (hash build for HashJoin), BitmapAnd/BitmapOr (bitmap combination)

---

## Part IV -- TupleTableSlot Abstraction

**Estimated size: 4,000 words | Depth: Detailed**

### 4.1 TupleTableSlot Structure
- `tuptable.h`: base `TupleTableSlot` fields: type, flags, nvalid, tts_ops, tupleDescriptor, values/isnull
- Flag bits: TTS_FLAG_EMPTY, TTS_FLAG_SHOULDFREE, TTS_FLAG_SLOW, TTS_FLAG_FIXED

### 4.2 Slot Type Hierarchy
- `VirtualTupleTableSlot` (TTSOpsVirtual): Datum/isnull arrays only
- `HeapTupleTableSlot` (TTSOpsHeapTuple): palloc'd physical tuples
- `MinimalTupleTableSlot` (TTSOpsMinimalTuple): compact tuples for hash operations
- `BufferHeapTupleTableSlot` (TTSOpsBufferHeapTuple): buffer-pinned disk tuples

### 4.3 TupleTableSlotOps Virtual Method Table
- `init`, `release`, `clear`, `getsomeattrs`, `getsysattr`
- `materialize`, `copyslot`, `get_heap_tuple`, `get_minimal_tuple`
- `copy_heap_tuple`, `copy_minimal_tuple`, `is_current_xact_tuple`

### 4.4 Slot Lifecycle Operations
- `ExecInitResultTupleSlotTL()`, `ExecInitScanTupleSlot()`, `MakeTupleTableSlot()`
- `ExecStoreHeapTuple()`, `ExecStoreBufferHeapTuple()`, `ExecStoreMinimalTuple()`, `ExecStoreVirtualTuple()`
- `ExecClearTuple()`, `ExecMaterializeSlot()`
- `ExecCopySlot()`, `ExecCopySlotHeapTuple()`, `ExecCopySlotMinimalTuple()`

### 4.5 Datum/Isnull Arrays and Deforming
- Lazy deforming via `slot_getsomeattrs()` / `slot_getsomeattrs_int()`
- `slot_getattr()`: fetch individual attributes
- `slot_getallattrs()`: force full deform

### 4.6 Buffer Manager Interaction
- `BufferHeapTupleTableSlot.buffer`: pin management
- `ExecStorePinnedBufferHeapTuple()`: caller retains pin

---

## Part V -- Expression Evaluation

**Estimated size: 5,000 words | Depth: Detailed**

### 5.1 Expression Tree Hierarchy
- `Expr` base (primnodes.h): Var, Const, Param, OpExpr, FuncExpr, BoolExpr
- ScalarArrayOpExpr, SubPlan, CaseExpr, CoalesceExpr, NullTest, etc.

### 5.2 ExprState and ExprEvalStep Compilation
- `ExecInitExpr()`: entry point for expression compilation
- `ExecInitExprRec()`: recursive compiler, maps Expr nodes to ExprEvalStep operations
- `ExprEvalStep` opcode array: EEOP_INNER_FETCHSOME, EEOP_FUNCEXPR, EEOP_QUAL, etc.
- Step types in `execExpr.h`: comprehensive opcode enumeration

### 5.3 ExecInitQual and Qualification
- `ExecInitQual()`: compiles AND-list of quals into a single ExprState
- EEOP_QUAL step: short-circuit FALSE return for qualification failure
- `ExecQual()`: inline wrapper evaluating the compiled qualification

### 5.4 Expression Interpretation
- `ExecInterpExpr()` (execExprInterp.c): main interpreter loop
- Computed goto dispatch (GCC) vs switch-based dispatch
- Step handlers for all opcode types
- `ExecInitInterpreter()`: one-time dispatch table setup

### 5.5 Projection
- `ExecBuildProjectionInfo()`: compiles target list into ProjectionInfo
- `ExecProject()`: inline wrapper executing projection into result slot
- Optimization: direct slot-to-slot copy when possible

### 5.6 JIT Expression Compilation
- Threshold-based JIT activation (`jit_above_cost`, `jit_optimize_above_cost`)
- `jit.c`: JIT provider interface
- `llvmjit_expr.c`: LLVM-based expression compilation
- Compiled function replaces `ExecInterpExpr` as the `evalfunc`

---

## Part VI -- Memory Context Management in Executor

**Estimated size: 3,000 words | Depth: Detailed**

### 6.1 Per-Query Memory Context
- `estate->es_query_cxt`: lifetime of entire executor invocation
- Created by `CreateExecutorState()`; destroyed by `FreeExecutorState()`

### 6.2 Per-Tuple Memory Context
- `econtext->ecxt_per_tuple_memory`: reset per tuple to avoid leaks
- `ResetExprContext()` macro: critical per-tuple cleanup
- `ExecEvalExprSwitchContext()`: switches to per-tuple context before evaluation

### 6.3 ExprContext Structure
- Tuple slot references: `ecxt_scantuple`, `ecxt_innertuple`, `ecxt_outertuple`
- Memory contexts: `ecxt_per_query_memory`, `ecxt_per_tuple_memory`
- Parameter values: `ecxt_param_exec_vals`, `ecxt_param_list_info`
- Aggregate values: `ecxt_aggvalues`, `ecxt_aggnulls`

### 6.4 Memory Context Switches During Execution
- `ExecAssignExprContext()`: creation and assignment
- `CreateExprContext()` / `CreateWorkExprContext()` / `CreateStandaloneExprContext()`
- Per-node work memory management (sort, hash)

---

## Part VII -- Scan Node Infrastructure

**Estimated size: 5,000 words | Depth: Detailed**

### 7.1 ExecScan -- Generic Scan Loop
- `execScan.c`: fetch next tuple -> apply qual -> project
- `ExecScanAccessMtd` / `ExecScanRecheckMtd` function pointer types
- All 16 scan node types delegate to `ExecScan()` with their specific methods

### 7.2 ScanState Base Structure
- `ss_currentRelation`, `ss_currentScanDesc`, `ss_ScanTupleSlot`
- Inherited by all scan state types

### 7.3 Table AM Abstraction
- `table_beginscan()`, `table_scan_getnextslot()` (tableam.h)
- Pluggable storage: heap AM is the default; supports custom AM implementations

### 7.4 Index AM Abstraction
- `index_beginscan()`, `index_getnext_slot()` (indexam.c)
- Runtime key evaluation for parameterized index scans
- Index-only scan: visibility map check, index-only tuple return

### 7.5 Bitmap Scan Two-Phase Execution
- Phase 1: `BitmapIndexScan` builds `TIDBitmap` via `MultiExecBitmapIndexScan`
- `BitmapAnd` / `BitmapOr`: combine multiple bitmaps
- Phase 2: `BitmapHeapScan` fetches matching heap pages
- Prefetching and lossy bitmap pages

### 7.6 Scan Direction and Cursor Support
- `ForwardScanDirection`, `BackwardScanDirection`, `NoMovementScanDirection`
- `EXEC_FLAG_BACKWARD`: backward scan capability requirement

---

## Part VIII -- Join Node Infrastructure

**Estimated size: 5,000 words | Depth: Detailed**

### 8.1 JoinState Base
- `jointype`, `joinqual`, `single_match`, `nl_NeedNewOuter`
- Join type dispatch: INNER, LEFT, RIGHT, FULL, SEMI, ANTI, RIGHT_ANTI

### 8.2 NestLoop
- Simple nested iteration: outer tuple drives inner rescans
- Parameterized inner: `NestLoopParam` for passing outer values
- `ExecReScan()` on inner plan for each outer tuple (or parameter change)
- Efficient for small inner or indexed inner plans

### 8.3 MergeJoin
- State machine with EXEC_MJ_* states (INITIALIZE_*, JOINTUPLES, NEXTOUTER, NEXTINNER, etc.)
- Requires sorted input on join keys
- Mark/restore protocol for handling duplicates
- `MergeJoinClause` comparison infrastructure

### 8.4 HashJoin
- Two-phase execution: build phase (`MultiExecHash`) and probe phase
- `ExecHashJoinImpl()`: state machine with HJ_BUILD_HASHTABLE, HJ_NEED_NEW_OUTER, HJ_SCAN_BUCKET, etc.
- Multi-batch hybrid hash join: spill to disk when memory exhausted
- `ExecHashTableCreate()`, `ExecScanHashBucket()`, `ExecHashJoinNewBatch()`
- Skew optimization for frequent hash values
- Parallel hash join: shared hash table, barrier synchronization

### 8.5 Outer Join Handling
- Null-extension of unmatched tuples
- HJ_FILL_OUTER_TUPLE / HJ_FILL_INNER_TUPLES states

### 8.6 Semi-Join and Anti-Join
- Early termination on first match (semi) or no match (anti)
- JOIN_SEMI, JOIN_ANTI, JOIN_RIGHT_ANTI optimization paths

---

## Part IX -- Aggregation and Grouping

**Estimated size: 5,000 words | Depth: Detailed**

### 9.1 AggState Structure
- Transition functions, combine functions, final functions
- Per-aggregate state: `AggStatePerAgg`, `AggStatePerTrans`, `AggStatePerGroup`
- Aggregate phases for GROUPING SETS

### 9.2 Aggregation Strategies
- `AGG_PLAIN`: no grouping keys, single group
- `AGG_SORTED`: pre-sorted input, advance on group boundary
- `AGG_HASHED`: hash-based grouping
- `AGG_MIXED`: combination for GROUPING SETS (sorted phases + hash phases)

### 9.3 Hash Aggregation
- Hash table construction via `BuildTupleHashTable()`
- Memory management: `hash_agg_check_limits()`, spill to disk
- `agg_fill_hash_table()` / `agg_retrieve_hash_table()`

### 9.4 GroupAggregate (Sorted)
- `agg_retrieve_direct()`: sorted-input advance logic
- Group boundary detection via comparison functions

### 9.5 WindowAgg
- Window frame management: ROWS, RANGE, GROUPS
- Tuplestore for buffering partition data
- Window function evaluation: `WinGetFuncArgInPartition()` etc.
- Run conditions for optimization
- Frame head/tail position tracking

### 9.6 GROUPING SETS / CUBE / ROLLUP
- Multiple aggregation phases
- Per-phase hash tables or sort groups
- Transition between phases

---

## Part X -- ModifyTable and Data Modification

**Estimated size: 6,000 words | Depth: Detailed**

### 10.1 ModifyTable Node Architecture
- Single node handling INSERT, UPDATE, DELETE, MERGE via `ExecModifyTable()`
- Outer plan provides tuples; operation dispatched per-tuple

### 10.2 INSERT Execution
- `ExecInsert()`: constraints, index insertion, triggers
- Batch insert optimization via `ExecBatchInsert()`
- Partition routing: `ExecPrepareTupleRouting()` -> `ExecFindPartition()`

### 10.3 UPDATE Execution
- `ExecUpdate()`: prologue/act/epilogue decomposition
- `ExecUpdatePrologue()`, `ExecUpdateAct()`, `ExecUpdateEpilogue()`
- Cross-partition update: `ExecCrossPartitionUpdate()` (delete + insert)

### 10.4 DELETE Execution
- `ExecDelete()`: prologue/act/epilogue decomposition
- Concurrent modification handling via `table_tuple_delete()` return codes

### 10.5 MERGE Execution
- `ExecMerge()`: dispatch to MATCHED/NOT MATCHED
- `ExecMergeMatched()`: conditions -> UPDATE/DELETE/DO NOTHING
- `ExecMergeNotMatched()`: conditions -> INSERT/DO NOTHING

### 10.6 Trigger Execution
- BEFORE/AFTER row and statement triggers
- `fireBSTriggers()` / `fireASTriggers()`
- Trigger transition tables: `ExecSetupTransitionCaptureState()`

### 10.7 ON CONFLICT (UPSERT)
- `ExecOnConflictUpdate()`: speculative insertion protocol
- Conflict detection via `ExecCheckIndexConstraints()`
- Arbiter index specification

### 10.8 RETURNING Clause
- `ExecProcessReturning()`: how modified tuples are returned to caller

### 10.9 Foreign Table Modification
- FDW callbacks for remote DML: `ExecForeignInsert`, `ExecForeignUpdate`, `ExecForeignDelete`

---

## Part XI -- Parallel Query Execution

**Estimated size: 4,000 words | Depth: Detailed**

### 11.1 Parallel-Aware and Parallel-Safe Classification
- Planner annotations on plan nodes
- `parallel_aware` flag for nodes that actively participate

### 11.2 Gather / GatherMerge
- `ExecGather()`: collect tuples from workers + optional local execution
- `ExecGatherMerge()`: merge-sort from multiple worker queues

### 11.3 Parallel Infrastructure
- `ExecParallelInitializeDSM()`: DSM segment setup per node
- `ExecParallelCreateReaders()`: TupleQueueReader creation
- `ExecParallelFinish()` / `ExecParallelCleanup()`: resource release

### 11.4 Worker Execution
- `ParallelQueryMain()`: worker entry point
- Worker gets serialized plan, creates independent executor state
- Results written to `TupleQueueFunnel` shared memory

### 11.5 Shared State
- Parallel bitmap heap scan: shared TIDBitmap iterator
- Parallel hash join: shared hash table with barrier synchronization
- Parallel sort: shared tuplesort state
- Parallel aggregation: partial aggregate + finalize

---

## Part XII -- Executor Interaction with Planner Output

**Estimated size: 3,000 words | Depth: Moderate**

### 12.1 Plan Tree Structure
- `PlannedStmt` -> `Plan` tree with lefttree/righttree
- Target lists, quals, and plan-level parameters

### 12.2 Parameterized Plans
- `Param` nodes: PARAM_EXTERN (user parameters) vs PARAM_EXEC (internal)
- NestLoop parameter passing: `NestLoopParam` list
- `chgParam` bitmapset for rescan triggering

### 12.3 InitPlan and SubPlan
- InitPlan: uncorrelated subquery, executed once, result stored in `es_param_exec_vals`
- SubPlan: correlated subquery, re-executed per outer tuple
- `ExecInitSubPlan()` / `ExecSubPlan()`

### 12.4 Plan-Level TargetList vs Executor Projection
- Plan.targetlist defines output columns
- `ExecBuildProjectionInfo()` compiles projection
- Simple tlist optimization: skip projection when input matches output

### 12.5 Runtime Partition Pruning
- `ExecInitPartitionPruning()`: startup pruning during ExecInitNode
- Runtime pruning: re-evaluate partition constraints when parameters change

---

## Part XIII -- SPI (Server Programming Interface)

**Estimated size: 3,000 words | Depth: Moderate**

### 13.1 SPI Connection Lifecycle
- `SPI_connect()` / `SPI_connect_ext()`: setup
- `SPI_finish()`: teardown
- Nested SPI connections for PL/pgSQL calling functions

### 13.2 Query Execution
- `SPI_execute()`: parse + plan + execute SQL string
- `SPI_execute_plan()`: execute pre-prepared plan with parameters
- `SPI_exec()`: convenience wrapper

### 13.3 Plan Preparation
- `SPI_prepare()` / `SPI_prepare_extended()`: parse + plan
- `SPI_keepplan()`: make plan survive `SPI_finish()`

### 13.4 Memory Management
- SPI memory context stack
- `SPI_palloc()`, `SPI_freetuple()`, `SPI_copytuple()`
- Tuple table management: `SPI_tuptable`

### 13.5 Usage in PL/pgSQL and Triggers
- PL/pgSQL statements compile to SPI calls
- Trigger functions use SPI for internal SQL
- User-defined functions (C, SQL, PL/*) as SPI clients

---

## Appendices

### A. Source File Map
| File | Purpose | Size |
|------|---------|------|
| execMain.c | Top-level executor lifecycle (92KB) | ExecutorStart/Run/Finish/End, InitPlan, EvalPlanQual |
| execProcnode.c | Node dispatch: Init/Proc/End (27KB) | ExecInitNode, ExecEndNode, MultiExecProcNode |
| execScan.c | Generic scan loop (9KB) | ExecScan, ExecScanReScan |
| execExpr.c | Expression compilation (138KB) | ExecInitExpr, ExecInitExprRec |
| execExprInterp.c | Expression interpretation (148KB) | ExecInterpExpr |
| execTuples.c | Tuple table slot management (66KB) | Slot creation, storage, deforming |
| execUtils.c | Executor utilities (39KB) | EState, ExprContext management |
| execParallel.c | Parallel query infrastructure (48KB) | DSM setup, worker coordination |
| execPartition.c | Partition routing (80KB) | ExecFindPartition, runtime pruning |
| execGrouping.c | Hash table for grouping (17KB) | BuildTupleHashTable |
| execIndexing.c | Index tuple insertion (36KB) | ExecInsertIndexTuples |
| execAmi.c | Auxiliary methods (17KB) | ExecReScan, ExecMarkPos |
| execSRF.c | Set-returning functions (29KB) | ExecInitTableFunctionResult |
| nodeModifyTable.c | Data modification (161KB) | INSERT/UPDATE/DELETE/MERGE |
| nodeAgg.c | Aggregation (150KB) | All aggregation strategies |
| nodeWindowAgg.c | Window aggregation (116KB) | Window frame management |
| nodeHash.c | Hash table for joins (112KB) | Hash build/probe |
| nodeHashjoin.c | Hash join execution (53KB) | ExecHashJoinImpl |
| nodeMergejoin.c | Merge join execution (50KB) | ExecMergeJoin state machine |
| nodeIndexscan.c | Index scan (52KB) | Runtime key handling |
| spi.c | Server Programming Interface (88KB) | SPI_connect/execute/prepare |
| instrument.c | EXPLAIN ANALYZE support (9KB) | InstrAlloc/Start/Stop |

### B. Critical Path Summary
1. **Executor Startup**: PortalRunSelect -> ExecutorStart -> CreateExecutorState -> InitPlan -> ExecInitNode
2. **Tuple Fetch**: ExecutePlan -> ExecProcNode -> ExecSeqScan -> ExecScan -> ExecQual -> ExecProject
3. **Hash Join**: ExecHashJoinImpl -> MultiExecHash -> ExecHashTableCreate -> ExecScanHashBucket
4. **Aggregation**: ExecAgg -> agg_fill_hash_table -> ExecProcNode -> agg_retrieve_hash_table
5. **ModifyTable**: ExecModifyTable -> ExecPrepareTupleRouting -> ExecInsert -> ExecConstraints -> ExecInsertIndexTuples
6. **Parallel Gather**: ExecGather -> ExecParallelInitializeDSM -> ExecParallelCreateReaders -> ParallelQueryMain
7. **Expression Eval**: ExecInitExpr -> ExecInitExprRec -> ExecEvalExpr -> ExecInterpExpr
8. **Nested Loop Join**: ExecNestLoop -> ExecProcNode (outer) -> ExecReScan (inner) -> ExecProcNode (inner) -> ExecQual
9. **Merge Join**: ExecMergeJoin -> state machine (EXEC_MJ_*) -> ExecProcNode -> ExecQual -> ExecProject
10. **Executor Shutdown**: ExecutorFinish -> ExecutorEnd -> ExecEndPlan -> ExecEndNode -> FreeExecutorState
