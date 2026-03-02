# Appendix B: Executor Terminology Glossary

**PostgreSQL 17.6 Executor Subsystem**

---

## A

**AGG_HASHED**
: An aggregation strategy where the executor builds a hash table keyed on
  grouping columns. All input tuples are consumed in one pass, then results are
  retrieved from the hash table. Supports spill-to-disk via `LogicalTape` when
  hash tables exceed memory limits. See: `ExecAgg`, `agg_fill_hash_table`.

**AGG_MIXED**
: An aggregation strategy combining both hashed and sorted phases, used for
  `GROUPING SETS`, `ROLLUP`, and `CUBE` queries. Hash phases handle grouping
  sets that lack common sort orderings; sorted phases handle the rest.

**AGG_PLAIN**
: The simplest aggregation strategy: no grouping, producing a single result row
  from all input tuples. Used for queries like `SELECT count(*) FROM t`.

**AGG_SORTED**
: An aggregation strategy where the input is pre-sorted on grouping columns.
  Group boundaries are detected by comparing adjacent tuples' key values.

**AggState**
: The runtime state node for aggregation (`T_Agg`). Contains the aggregation
  strategy, per-aggregate function state, hash tables, phase information for
  `GROUPING SETS`, and spill management for hash aggregation.

## B

**BufFile**
: A temporary file abstraction used for hash join batch overflow and aggregate
  hash spill. Backed by temporary files managed by PostgreSQL's file manager.

## C

**chgParam**
: A `Bitmapset` in each `PlanState` node indicating which `PARAM_EXEC` parameters
  have changed since the last execution. Used by `ExecReScan` to determine
  whether a rescan is necessary.

## D

**Deforming (Tuple Deforming)**
: The process of extracting individual attribute values from a tuple's on-disk
  format into separate Datum/isnull arrays. Performed lazily by
  `slot_getsomeattrs()` and can be JIT-compiled for performance. The
  `EEOP_*_FETCHSOME` steps in expression evaluation trigger deforming.

**DestReceiver**
: An abstraction for tuple output destinations. The executor sends result tuples
  to the `DestReceiver` via its `receiveSlot()` callback. Common implementations:
  `DestRemote` (send to client), `DestSPI` (store for SPI), `DestTuplestore`
  (buffer in memory).

**DSM (Dynamic Shared Memory)**
: Shared memory segments allocated per-query for parallel execution. Contains
  the serialized plan, tuple queues, and per-node coordination state. Created by
  `ExecInitParallelPlan()` and accessed by workers via `shm_toc` lookups.

## E

**EEOP_* (Expression Evaluation Opcodes)**
: The opcode constants for `ExprEvalStep` operations. Examples include
  `EEOP_INNER_VAR` (read attribute from inner tuple), `EEOP_FUNCEXPR` (call a
  function), `EEOP_QUAL` (short-circuit boolean check), and `EEOP_DONE` (end of
  step array).

**eflags**
: A bitmask of `EXEC_FLAG_*` constants passed to `ExecutorStart` and
  `ExecInitNode`. Controls execution capabilities:
  - `EXEC_FLAG_EXPLAIN_ONLY`: Do not actually execute
  - `EXEC_FLAG_BACKWARD`: Support backward scan
  - `EXEC_FLAG_REWIND`: Support rescan without parameter changes
  - `EXEC_FLAG_MARK`: Support mark/restore
  - `EXEC_FLAG_SKIP_TRIGGERS`: Do not fire triggers

**EPQ (EvalPlanQual)**
: A mechanism for rechecking tuples under `READ COMMITTED` isolation. When a
  DML operation encounters a concurrently modified row, EPQ re-evaluates the
  query predicate against the updated version of the row to determine if it
  still qualifies.

**EState (Executor State)**
: The per-query execution state shared by all plan nodes. Contains MVCC
  snapshots, the range table, result relations, parameter storage, memory
  contexts, and the tuple table. Created by `CreateExecutorState()` and
  destroyed by `FreeExecutorState()`.

**ExecProcNodeMtd**
: The function pointer type for the per-node execution function. Stored in
  `PlanState.ExecProcNode`. The Volcano model calls through this pointer to
  retrieve tuples from any node type.

**ExprContext**
: The runtime evaluation context for expressions. Provides three tuple slots
  (`ecxt_scantuple`, `ecxt_innertuple`, `ecxt_outertuple`), two memory contexts
  (per-query and per-tuple), and parameter/aggregate value arrays. Allocated per
  node via `CreateExprContext()`.

**ExprState**
: The compiled representation of a SQL expression. Contains a flat array of
  `ExprEvalStep` operations and a function pointer (`evalfunc`) that dispatches
  to the interpreter, JIT-compiled code, or a fast-path function.

## F

**Fast-Path Functions**
: Optimized evaluation functions for trivially simple expressions. Examples:
  `ExecJustConst` (return a constant), `ExecJustInnerVar` (read a single
  attribute from the inner tuple slot). Selected during `ExecReadyExpr()` to
  bypass interpreter overhead.

## G

**Gather / GatherMerge**
: Executor nodes that collect tuples from parallel workers. `Gather` returns
  tuples in arbitrary order; `GatherMerge` preserves sort order using a binary
  heap merge. Both support "leader as worker" local execution.

## H

**HashJoinTable**
: The in-memory hash table structure for hash joins. Contains the bucket array,
  batch management state, skew buckets for frequent values, and memory accounting.
  Created by `ExecHashTableCreate()`.

**Hybrid Hash Join**
: PostgreSQL's hash join algorithm that gracefully handles inner relations larger
  than `work_mem` by partitioning into multiple batches. Only the current batch
  resides in memory; other batches overflow to temporary `BufFile`s.

## I

**InstrCountFiltered1 / InstrCountFiltered2**
: Macros used for `EXPLAIN ANALYZE` statistics in join nodes. `Filtered1` counts
  tuples rejected by `joinqual`; `Filtered2` counts tuples that passed `joinqual`
  but failed `otherqual`.

## J

**JIT (Just-In-Time Compilation)**
: LLVM-based compilation of expression evaluation and tuple deforming into native
  machine code. Triggered when query cost exceeds `jit_above_cost`. The JIT
  pipeline: generate LLVM IR from step array, optionally inline and optimize,
  compile to machine code, replace `evalfunc` pointer.

**joinqual**
: The primary join condition stored in `JoinState.joinqual`. Determines whether
  two tuples "match" for join purposes. Distinct from `otherqual` (`ps.qual`),
  which is a secondary filter applied to matched pairs.

**JunkFilter**
: Removes "junk" attributes (like `ctid` for UPDATE/DELETE) from result tuples
  before sending them to the client. Created by `ExecInitJunkFilter()`.

## L

**Leader Participation**
: In parallel query, the leader process also executes the parallel plan subtree
  locally to avoid wasting a CPU core. Implemented in `gather_getnext()` which
  alternates between worker queue reads and local `ExecProcNode()` calls.

## M

**Mark/Restore**
: A protocol used by MergeJoin to handle duplicate merge keys. `ExecMarkPos()`
  saves the inner scan position; `ExecRestrPos()` rewinds to it. This avoids
  rescanning the entire inner relation for each duplicate in the outer.

**Materialization**
: The process of buffering all tuples from a child plan into memory (or a
  tuplestore that may spill to disk). Performed by the `Material` node and
  used internally by `Sort`, `WindowAgg`, and `RecursiveUnion`.

**MERGE Command**
: SQL standard command combining INSERT, UPDATE, and DELETE into a single
  statement. The executor's `ExecMerge()` determines whether each source row
  matches a target row, then dispatches to the appropriate action.

## N

**Null-Extension**
: For outer joins (LEFT, RIGHT, FULL, ANTI), when a tuple has no match in the
  other relation, a "fake" joined tuple is created by combining the real tuple
  with a pre-allocated all-NULLs slot (`nl_NullInnerTupleSlot` etc.).

## O

**otherqual**
: Secondary qualification stored in `PlanState.qual`. In join nodes, this is the
  filter applied after `joinqual` passes. Tuples that pass `joinqual` but fail
  `otherqual` are counted by `InstrCountFiltered2`.

## P

**PARAM_EXEC**
: Internal parameters used for cross-node communication within a single query.
  Used by NestLoop to pass outer tuple values to parameterized inner scans, and
  by SubPlan to pass correlated values. Stored in `es_param_exec_vals`.

**Parallel-Aware**
: A plan node that actively coordinates work distribution among parallel workers
  (e.g., Parallel Seq Scan distributes pages, Parallel Hash Join shares the hash
  table). Implements `ExecParallelEstimate`, `ExecParallelInitializeDSM`, and
  `ExecParallelInitializeWorker` callbacks.

**Parallel-Safe**
: A plan node that can execute correctly in a parallel worker process. All
  parallel-safe nodes below a `Gather` node run identical copies in each worker.
  A parallel-safe but not parallel-aware node does not coordinate with other
  workers.

**PlanState**
: The runtime counterpart of a `Plan` node. Contains the `ExecProcNode` function
  pointer, links to child state nodes, qualification expressions, result slot,
  projection info, and instrumentation data. Base type for all `*State` nodes.

**Projection**
: The process of computing output columns from input tuples by evaluating target
  list expressions. Performed by `ExecProject()` using a compiled `ExprState`
  that writes results directly into the result slot's Datum/isnull arrays.

## Q

**Qualification**
: The boolean expression that filters tuples (WHERE clause, JOIN condition).
  Evaluated by `ExecQual()` using the compiled qualification expression. NULL
  results are treated as FALSE (standard SQL three-valued logic).

**QueryDesc (Query Descriptor)**
: The bridge structure between the traffic cop and executor. Carries the planned
  statement, MVCC snapshots, parameters, destination receiver, and (after
  `ExecutorStart`) the `EState` and root `PlanState`.

## R

**Rescan**
: Resetting a plan node to produce its output again from the beginning. Triggered
  by `ExecReScan()`, which checks `chgParam` to determine if a re-evaluation is
  needed. Used by NestLoop (inner rescan for each outer tuple) and by SubPlan
  (re-evaluate for each outer row).

**ResultRelInfo**
: Per-target-table state for DML operations. Contains the open `Relation`,
  index descriptors, trigger descriptors, constraint expressions, RETURNING
  projection, and ON CONFLICT state.

## S

**single_match**
: An optimization flag in `JoinState` set for semi-joins and inner-unique joins.
  When true, the join advances to the next outer tuple after finding the first
  match, avoiding unnecessary duplicate processing.

**SPI (Server Programming Interface)**
: An API allowing C functions (triggers, procedural languages) to execute SQL
  queries within the server. `SPI_connect()` establishes a connection;
  `SPI_execute()` runs a query; `SPI_finish()` cleans up.

## T

**Transition Table**
: A feature of AFTER triggers that provides access to all rows modified by the
  current statement as a virtual table. Created using `REFERENCING OLD TABLE AS`
  / `NEW TABLE AS` syntax. Implemented as tuplestores filled during
  `ExecModifyTable`.

**TupleTableSlot**
: The core tuple storage abstraction in the executor. Provides a uniform interface
  over four storage formats: Virtual (Datum arrays), HeapTuple, MinimalTuple, and
  BufferHeapTuple. The `tts_ops` virtual method table dispatches format-specific
  operations.

**TupleTableSlotOps**
: The virtual method table defining operations on a `TupleTableSlot`: `init`,
  `release`, `clear`, `getsomeattrs`, `getsysattr`, `materialize`, `copyslot`,
  `get_heap_tuple`, `get_minimal_tuple`, `copy_heap_tuple`, `copy_minimal_tuple`.

## V

**Virtual Tuple**
: A tuple representation using parallel Datum and isnull arrays without a
  contiguous on-disk format. Used for projected result tuples and constant
  expressions. Operated by `TTSOpsVirtual`.

**Volcano Model (Iterator Model)**
: The execution model where each plan node provides a `next-tuple` function
  (`ExecProcNode`). The root node pulls tuples from children, which recursively
  pull from their children, forming a demand-driven pipeline. Named after the
  Volcano database system where this model was formalized.

## W

**work_mem**
: The GUC parameter controlling memory available for sort and hash operations.
  Directly affects hash join batch count, aggregate hash table spill threshold,
  and sort operation memory. Each operation gets its own `work_mem` allocation.

**Worker (Parallel Worker)**
: A background process launched to execute a parallel portion of a query plan.
  Enters via `ParallelQueryMain()`, deserializes the plan from DSM, runs it,
  and sends results through a shared memory tuple queue.
