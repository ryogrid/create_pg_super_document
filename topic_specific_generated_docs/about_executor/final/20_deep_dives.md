# Deep Dives: Advanced Executor Topics

**PostgreSQL 17.6 Executor Subsystem**

This chapter covers advanced implementation details that go beyond the standard
component documentation. Each section examines a specific mechanism in depth,
with source-level analysis and practical implications.

---

## Table of Contents

1. [HashJoin Batch Management and work_mem Overflow](#1-hashjoin-batch-management-and-work_mem-overflow)
2. [Aggregate Node: Four-Strategy Dispatch and Hash Spill-to-Disk](#2-aggregate-node-four-strategy-dispatch-and-hash-spill-to-disk)
3. [MergeJoin State Machine: Complete EXEC_MJ_* Transition Analysis](#3-mergejoin-state-machine-complete-exec_mj-transition-analysis)
4. [Parallel Query Shared State](#4-parallel-query-shared-state)
5. [JIT Compilation Pipeline](#5-jit-compilation-pipeline)
6. [Trigger Execution Ordering and Transition Table Management](#6-trigger-execution-ordering-and-transition-table-management)
7. [Executor Hooks and Extensibility](#7-executor-hooks-and-extensibility)
8. [Interaction Between Executor and Buffer Manager](#8-interaction-between-executor-and-buffer-manager)

---

## 1. HashJoin Batch Management and work_mem Overflow

**Source**: `src/backend/executor/nodeHashjoin.c`, `src/backend/executor/nodeHash.c`

### The Hybrid Hash Join Algorithm

PostgreSQL implements Hybrid Hash Join, which gracefully degrades when the inner
relation exceeds `work_mem`. The algorithm partitions both relations into batches
based on hash values, processing one batch at a time.

### Batch Lifecycle

**Initial Configuration**: During `ExecHashTableCreate()`, the executor estimates
the number of batches needed based on the inner relation's estimated size and
`work_mem`. The batch count is always a power of 2.

```
nbatch = 1                    -- start optimistic (all in memory)
while (inner_size / nbatch > work_mem):
    nbatch *= 2               -- double until it fits
```

**Hash Value Partitioning**: Each tuple's batch assignment is computed from its
hash value:

```c
/* From nodeHash.c */
batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
```

The high-order bits of the hash value (above the bucket bits) determine the batch.
Only batch 0 tuples are kept in memory; all others are saved to temporary
`BufFile`s on disk.

### Dynamic Batch Increase

If the in-memory hash table grows beyond `work_mem` during build, the executor
increases the batch count at runtime:

1. `ExecHashIncreaseNumBatches()` doubles `nbatch`
2. Existing in-memory tuples that now belong to higher batches are evicted to
   their batch files
3. The process can repeat until the in-memory portion fits in `work_mem`
4. In the worst case, each batch holds approximately `work_mem / nbatch` data

**Skew Optimization**: For inner relations with highly skewed distributions, the
executor maintains special "skew buckets" for the most common values (MCVs from
`pg_statistic`). These buckets are checked first during probe and prevent
pathological hash chain lengths.

### Multi-Batch Probe Phase

After building batch 0's hash table:

1. Outer tuples for batch 0 are probed directly
2. Outer tuples for other batches are saved to their respective batch files
3. When batch 0 is complete, `ExecHashJoinNewBatch()` processes remaining batches:
   - Loads the inner batch file into a fresh hash table
   - Scans the corresponding outer batch file for probing
   - Skips batches where either file is empty (optimization)

### Performance Implications

| Scenario | Behavior |
|----------|----------|
| Inner fits in `work_mem` | Single batch, pure in-memory hash join O(N+M) |
| Inner slightly exceeds `work_mem` | 2-4 batches, moderate I/O overhead |
| Inner much larger than `work_mem` | Many batches, significant temp file I/O |
| Extreme skew | Skew buckets prevent degradation of hot keys |

The `work_mem` GUC directly controls this boundary. Increasing `work_mem` reduces
the probability of multi-batch execution. Monitor `EXPLAIN (ANALYZE, BUFFERS)` for
`Batches: N` in hash join output to detect multi-batch behavior.

---

## 2. Aggregate Node: Four-Strategy Dispatch and Hash Spill-to-Disk

**Source**: `src/backend/executor/nodeAgg.c`

### Strategy Dispatch

The `ExecAgg` function dispatches to different execution paths based on the
aggregation strategy chosen by the planner:

```
ExecAgg(pstate)
    |
    +-- AGG_PLAIN:   agg_retrieve_direct()    -- no grouping, single result
    +-- AGG_SORTED:  agg_retrieve_direct()    -- pre-sorted input, sequential groups
    +-- AGG_HASHED:  agg_fill_hash_table()    -- hash table for all groups
    |                agg_retrieve_hash_table()
    +-- AGG_MIXED:   agg_fill_hash_table()    -- hash phase first (for some grouping sets)
                     agg_retrieve_hash_table() -- then sorted phase
                     agg_retrieve_direct()
```

### AGG_PLAIN: No Grouping

Processes all input tuples into a single group. Calls `advance_aggregates()` for
each tuple, then `finalize_aggregates()` once to produce exactly one result row.
Used for queries like `SELECT count(*) FROM t`.

### AGG_SORTED: Pre-Sorted Input

The child plan guarantees sort order on grouping keys. The executor detects group
boundaries by comparing the current tuple's grouping keys against the previous
tuple using `ExecQualAndReset()` on precompiled equality functions.

When a boundary is detected:
1. Finalize the current group
2. Return the result tuple
3. On the next call, initialize new transition values and continue

### AGG_HASHED: Hash Aggregation

All input tuples are consumed in a single pass during `agg_fill_hash_table()`.
Each tuple is looked up in a `TupleHashTable` (based on `simplehash`). If the
group exists, its transition values are updated. If not, a new entry is created.

### Hash Aggregation Spill-to-Disk

When the hash table exceeds the memory threshold (`hash_mem_multiplier * work_mem`),
the executor spills excess tuples to disk:

**Spill Mechanism**:
1. `hash_agg_check_limits()` is called after each tuple insertion
2. When memory exceeds the threshold, `hash_agg_enter_spill_mode()` creates
   a `HashAggSpill` structure with `LogicalTape`s
3. Tuples for groups not yet in the hash table are written to tapes, partitioned
   by hash value bits
4. After the first pass completes, `agg_refill_hash_table()` processes spilled
   partitions one at a time

**Recursive Spill**: If a spilled partition still exceeds memory when reloaded,
it is spilled again to finer-grained partitions. This recursion guarantees
completion regardless of data size, though with increasing I/O cost:

```
Pass 1:  Read all input, spill excess to N partitions
Pass 2:  For each spilled partition:
           Load into hash table
           If fits: process and emit results
           If exceeds: spill again to finer partitions
Pass 3+: Continue until all partitions processed
```

### AGG_MIXED: GROUPING SETS

`AGG_MIXED` handles queries with `GROUPING SETS`, `ROLLUP`, or `CUBE`. The planner
creates multiple "phases":

1. **Hash phases**: Grouping sets that share no common sort order use hash
   aggregation. Multiple hash tables are populated simultaneously (one per
   grouping set).
2. **Sorted phases**: Grouping sets compatible with the input sort order use
   sorted aggregation, processed sequentially after the hash phase.

Each input tuple is fed to all active hash tables simultaneously, then to the
sorted phase as needed. The `numphases` and `phase` fields in `AggState` track
the current phase during result retrieval.

---

## 3. MergeJoin State Machine: Complete EXEC_MJ_* Transition Analysis

**Source**: `src/backend/executor/nodeMergejoin.c`

### State Definitions

```c
#define EXEC_MJ_INITIALIZE_OUTER    1
#define EXEC_MJ_INITIALIZE_INNER    2
#define EXEC_MJ_JOINTUPLES          3
#define EXEC_MJ_NEXTOUTER           4
#define EXEC_MJ_TESTOUTER           5
#define EXEC_MJ_NEXTINNER           6
#define EXEC_MJ_SKIP_TEST           7
#define EXEC_MJ_SKIPOUTER_ADVANCE   8
#define EXEC_MJ_SKIPINNER_ADVANCE   9
#define EXEC_MJ_ENDOUTER            10
#define EXEC_MJ_ENDINNER            11
```

### Complete State Transition Diagram

```
    START
      |
      v
  [1: INITIALIZE_OUTER]
      |
      +-- outer NULL/NONMATCHABLE (LEFT join) --> emit null-fill, stay at 1
      +-- outer MATCHABLE --> [2: INITIALIZE_INNER]
      +-- outer ENDOFJOIN --> [10: ENDOUTER] (RIGHT join) or DONE

  [2: INITIALIZE_INNER]
      |
      +-- inner NULL/NONMATCHABLE (RIGHT join) --> emit null-fill, stay at 2
      +-- inner MATCHABLE --> [7: SKIP_TEST]
      +-- inner ENDOFJOIN --> [11: ENDINNER] (LEFT join) or DONE

  [7: SKIP_TEST]  (main synchronization state)
      |
      +-- MJCompare: outer == inner --> mark inner, goto [3: JOINTUPLES]
      +-- MJCompare: outer < inner  --> [8: SKIPOUTER_ADVANCE]
      +-- MJCompare: outer > inner  --> [9: SKIPINNER_ADVANCE]

  [3: JOINTUPLES]
      | (set next state = NEXTINNER before testing quals)
      +-- joinqual passes:
      |     +-- ANTI join: goto [4: NEXTOUTER]
      |     +-- single_match: goto [4: NEXTOUTER]
      |     +-- otherqual passes: RETURN projected tuple
      |     +-- otherqual fails: goto [6: NEXTINNER]
      +-- joinqual fails: goto [6: NEXTINNER]

  [4: NEXTOUTER]
      | (emit null-fill if doFillOuter && !mj_MatchedOuter)
      +-- fetch next outer
      +-- outer MATCHABLE --> [5: TESTOUTER]
      +-- outer ENDOFJOIN --> [10: ENDOUTER]

  [5: TESTOUTER]
      | (compare new outer against MARKED inner)
      +-- equal: restore inner to mark, goto [3: JOINTUPLES]
      +-- not equal: goto [7: SKIP_TEST]

  [6: NEXTINNER]
      | (emit null-fill if doFillInner && !mj_MatchedInner)
      +-- fetch next inner
      +-- inner equal to outer: goto [3: JOINTUPLES]
      +-- inner past outer: goto [4: NEXTOUTER]
      +-- inner ENDOFJOIN: goto [4: NEXTOUTER]

  [8: SKIPOUTER_ADVANCE]
      | (emit null-fill if doFillOuter && !mj_MatchedOuter)
      +-- fetch next outer
      +-- MATCHABLE: goto [7: SKIP_TEST]
      +-- ENDOFJOIN: goto [10: ENDOUTER]

  [9: SKIPINNER_ADVANCE]
      | (emit null-fill if doFillInner && !mj_MatchedInner)
      +-- perform extra mark if needed
      +-- fetch next inner
      +-- MATCHABLE: goto [7: SKIP_TEST]
      +-- ENDOFJOIN: goto [11: ENDINNER]

  [10: ENDOUTER]  (outer exhausted)
      +-- RIGHT/FULL join: emit unmatched inner tuples with null outer
      +-- inner exhausted: DONE

  [11: ENDINNER]  (inner exhausted)
      +-- LEFT/FULL join: emit unmatched outer tuples with null inner
      +-- outer exhausted: DONE
```

### The Mark/Restore Protocol for Duplicate Keys

When both relations contain duplicate merge keys, the join must produce the
Cartesian product of matching groups. The mark/restore protocol handles this
efficiently:

1. **Mark** (state 7, SKIP_TEST): When keys first match, `ExecMarkPos()` saves
   the inner scan position
2. **Consume** (states 3/6): Inner tuples are consumed one by one
3. **Test** (state 5, TESTOUTER): When advancing to a new outer tuple, compare
   against the marked inner position
4. **Restore** (state 5): If the new outer key equals the marked inner key,
   `ExecRestrPos()` rewinds the inner scan to the mark
5. **Advance mark** (state 7): When keys differ, the mark advances forward

This avoids rescanning the entire inner relation for each duplicate in the outer
relation, keeping complexity at O(N + M + |cross product of matching groups|).

### NULL Handling

Merge keys containing NULLs are classified as `MJEVAL_NONMATCHABLE` by
`MJEvalOuterValues()`/`MJEvalInnerValues()`. NULL keys never match each other
(correct SQL semantics). For outer joins, unmatched NULL-key tuples emit
null-extended rows.

---

## 4. Parallel Query Shared State

**Source**: `src/backend/executor/execParallel.c`, various node files

### Shared Hash Tables (Parallel Hash Join)

Parallel hash join coordinates multiple workers through a barrier-based protocol
using shared memory:

**Build Phase Barriers**:
```
PHJ_BUILD_ELECT       -- Workers race; one is elected
PHJ_BUILD_ALLOCATE    -- Elected worker allocates shared hash table
PHJ_BUILD_HASH_INNER  -- All workers hash inner tuples in parallel
PHJ_BUILD_HASH_OUTER  -- All workers partition outer tuples (multi-batch)
PHJ_BUILD_RUN         -- Build complete, probing begins
PHJ_BUILD_FREE        -- Elected worker frees shared resources
```

The shared hash table uses `dsa_allocate()` (Dynamic Shared Area) to allocate
memory visible to all workers. Each worker hashes a subset of inner tuples
(distributed via the parallel scan mechanism) and inserts into shared buckets
using atomic operations.

**Per-Batch Barriers**: Each batch has its own barrier with phases:
```
PHJ_BATCH_ELECT    -> PHJ_BATCH_ALLOCATE -> PHJ_BATCH_LOAD ->
PHJ_BATCH_PROBE    -> PHJ_BATCH_SCAN     -> PHJ_BATCH_FREE
```

Workers dynamically pick unprocessed batches, ensuring load balancing even when
batch sizes vary.

### Shared Sort (Parallel Sort)

`Sort` nodes in parallel plans do not share sort state. Instead, each worker
independently sorts its subset of tuples. The `GatherMerge` node above
performs an N-way merge of the pre-sorted streams using a binary heap.

For `IncrementalSort` in parallel plans, each worker performs its own incremental
sort on presorted groups, and the leader merges the results.

### Shared Bitmap (Parallel Bitmap Heap Scan)

Parallel bitmap heap scans share a `TBMSharedIterator` in DSM:

1. One worker builds the TIDBitmap (via BitmapIndexScan)
2. All workers iterate the bitmap using `tbm_shared_iterate()`, which atomically
   distributes pages among workers
3. Each worker fetches and filters tuples from its assigned heap pages
4. The `ParallelBitmapHeapState` coordinates through a barrier ensuring the
   bitmap is fully built before any worker begins scanning

### Shared Append (Parallel Append)

The `Append` node in parallel mode uses a shared counter to distribute child
subplans among workers. Each worker atomically claims the next unfinished
subplan index, ensuring all subplans are covered without duplication.

### DSM Segment Layout

All shared state is organized within a single DSM segment using a Table of
Contents (`shm_toc`):

```
+-------------------------------------------+
| shm_toc (Table of Contents)               |
+-------------------------------------------+
| FixedParallelExecutorState                 |
| Serialized PlannedStmt (text)              |
| ParamListInfo                              |
| Tuple Queues: shm_mq[nworkers]            |
| SharedExecutorInstrumentation              |
| BufferUsage[nworkers]                      |
| WalUsage[nworkers]                         |
| DSA handle                                 |
| Per-node parallel state (variable)         |
+-------------------------------------------+
```

---

## 5. JIT Compilation Pipeline

**Source**: `src/backend/jit/jit.c`, `src/backend/jit/llvmjit.c`,
`src/backend/jit/llvmjit_expr.c`

### When JIT Is Triggered

JIT compilation is controlled by three cost thresholds:

| GUC Parameter | Default | Effect |
|---------------|---------|--------|
| `jit_above_cost` | 100000 | Enable JIT if query cost exceeds this |
| `jit_inline_above_cost` | 500000 | Inline function bodies if cost exceeds this |
| `jit_optimize_above_cost` | 500000 | Apply LLVM optimization passes if cost exceeds this |

The planner sets `jit_flags` in `PlannedStmt` based on these thresholds. The
flags are a bitmask:

```c
#define PGJIT_NONE     0
#define PGJIT_PERFORM  (1 << 0)   /* JIT is enabled */
#define PGJIT_OPT3     (1 << 1)   /* Apply -O3 optimization */
#define PGJIT_INLINE   (1 << 2)   /* Inline function bodies */
#define PGJIT_EXPR     (1 << 3)   /* JIT-compile expressions */
#define PGJIT_DEFORM   (1 << 4)   /* JIT-compile tuple deforming */
```

### What Is Compiled

JIT compilation targets two primary areas:

**1. Expression Evaluation**: The `ExprEvalStep` array (normally interpreted by
`ExecInterpExpr`) is compiled into native machine code. Each step opcode
becomes a block of LLVM IR that operates directly on Datum values.

**2. Tuple Deforming**: The `slot_getsomeattrs()` function, which extracts
attribute values from on-disk tuple format, is JIT-compiled into a specialized
version that knows the exact tuple descriptor at compile time, eliminating
per-attribute type dispatch.

### LLVM IR Generation Pipeline

```
ExecReadyExpr()
    |
    +-- jit_compile_expr(state)
          |
          +-- llvm_compile_expr(state)
                |
                1. Create LLVM function
                2. For each ExprEvalStep:
                |    +-- Emit LLVM IR for the step opcode
                |    +-- Branch to next step
                3. Verify LLVM function
                4. Apply optimization passes (if PGJIT_OPT3)
                5. Compile to machine code
                6. Set state->evalfunc = compiled_function_ptr
```

**Step-to-IR Translation**: Each `EEOP_*` opcode has a corresponding LLVM IR
generation routine in `llvmjit_expr.c`. For example:
- `EEOP_INNER_VAR` becomes a direct load from the slot's `tts_values` array
- `EEOP_FUNCEXPR_STRICT` becomes a NULL check followed by a function call
- `EEOP_QUAL` becomes a branch that jumps to the "return false" block

### Inlining

When `PGJIT_INLINE` is set, the JIT compiler inlines function bodies from
precompiled bitcode files. PostgreSQL ships LLVM bitcode (`.bc` files) alongside
shared libraries. During inlining, functions like `int4eq()` or `text_cmp()` are
inlined into the expression evaluation function, eliminating function call overhead.

### Optimization Passes

When `PGJIT_OPT3` is set, LLVM's optimization pipeline runs on the generated IR:
- Dead code elimination
- Constant propagation
- Loop unrolling
- Instruction combining
- Memory-to-register promotion

### Performance Characteristics

JIT compilation has a fixed overhead (compilation time) that must be amortized
over many tuple evaluations. The cost thresholds ensure JIT is only used for
queries expected to process many tuples.

Typical speedups from JIT:
- Expression evaluation: 10-30% faster for complex expressions
- Tuple deforming: 10-50% faster for wide tables
- Inlined functions: Eliminates C function call overhead entirely

---

## 6. Trigger Execution Ordering and Transition Table Management

**Source**: `src/backend/executor/nodeModifyTable.c`,
`src/backend/commands/trigger.c`

### Trigger Execution Order

Triggers fire in a strict sequence during DML execution:

```
1. BEFORE STATEMENT triggers    (once, at start of ExecModifyTable)
   |
   v
2. For each tuple from subplan:
   a. BEFORE ROW triggers       (may modify or suppress the tuple)
   b. DML operation              (INSERT/UPDATE/DELETE via table AM)
   c. AFTER ROW triggers         (queued, not immediately fired)
   |
   v
3. AFTER STATEMENT triggers     (once, after all tuples processed)
```

### BEFORE ROW Trigger Behavior

BEFORE ROW triggers can:
- **Modify the tuple**: Return a different tuple to be inserted/updated
- **Suppress the operation**: Return NULL to skip this row entirely
- **Raise an error**: Abort the entire statement

Multiple BEFORE ROW triggers on the same event fire in alphabetical order by
trigger name. Each trigger sees the tuple as modified by the previous trigger.

### AFTER ROW Trigger Queuing

AFTER ROW triggers are not fired immediately. They are queued in the
`AfterTriggerEventList` and fired at the end of the statement (or at transaction
commit for `CONSTRAINT TRIGGER ... DEFERRABLE`).

The queue is processed by `AfterTriggerEndQuery()`, called from
`standard_ExecutorFinish()`.

### Transition Tables (OLD TABLE / NEW TABLE)

Transition tables provide triggers with access to the complete set of modified
rows, not just the current row:

```sql
CREATE TRIGGER audit_trigger
    AFTER INSERT ON orders
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_func();
```

**Implementation**:
1. During `ExecModifyTable`, each modified row is accumulated into a `Tuplestorestate`:
   - Old tuples (before modification) go into `es_trig_oldtup_store`
   - New tuples (after modification) go into `es_trig_newtup_store`
2. These tuplestores are made available to the trigger function as named
   tuplestores accessible via `SPI_register_trigger_data()`
3. Within the trigger function, `OLD TABLE` and `NEW TABLE` appear as read-only
   tables that can be queried with standard SQL

**Memory Management**: Transition table tuplestores live in the per-query memory
context and are cleaned up by `ExecutorEnd`. For large DML operations, the
tuplestores may spill to disk (controlled by `work_mem`).

### Partition-Specific Triggers

When modifying a partitioned table, triggers defined on individual partitions
fire in addition to triggers on the root table. The firing order is:
1. Root table BEFORE STATEMENT triggers
2. Partition BEFORE STATEMENT triggers (for each affected partition)
3. Per-row triggers (on the specific partition being modified)
4. Partition AFTER STATEMENT triggers
5. Root table AFTER STATEMENT triggers

---

## 7. Executor Hooks and Extensibility

**Source**: `src/include/executor/executor.h`, `src/backend/executor/execMain.c`

### Hook Mechanism

The executor provides four lifecycle hooks, each following the same pattern:

```c
/* From src/include/executor/executor.h */
typedef void (*ExecutorStart_hook_type) (QueryDesc *queryDesc, int eflags);
extern PGDLLIMPORT ExecutorStart_hook_type ExecutorStart_hook;

typedef void (*ExecutorRun_hook_type) (QueryDesc *queryDesc,
                                       ScanDirection direction,
                                       uint64 count,
                                       bool execute_once);
extern PGDLLIMPORT ExecutorRun_hook_type ExecutorRun_hook;

typedef void (*ExecutorFinish_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorFinish_hook_type ExecutorFinish_hook;

typedef void (*ExecutorEnd_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorEnd_hook_type ExecutorEnd_hook;
```

**Hook Dispatch Pattern** (identical for all four):

```c
void ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (ExecutorStart_hook)
        (*ExecutorStart_hook) (queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}
```

### Permission Check Hook

```c
typedef bool (*ExecutorCheckPerms_hook_type) (List *rangeTable,
                                              List *rteperminfos,
                                              bool abort);
extern PGDLLIMPORT ExecutorCheckPerms_hook_type ExecutorCheckPerms_hook;
```

This hook allows extensions (e.g., `sepgsql`) to implement custom access control
that supplements or replaces the built-in ACL checking.

### Notable Extensions Using Hooks

| Extension | Hooks Used | Purpose |
|-----------|-----------|---------|
| `pg_stat_statements` | ExecutorStart, ExecutorRun, ExecutorFinish, ExecutorEnd | Query statistics collection |
| `auto_explain` | ExecutorStart, ExecutorEnd | Automatic query plan logging |
| `pg_hint_plan` | ProcessUtility_hook (not executor) | Plan hint injection |
| `sepgsql` | ExecutorCheckPerms | SELinux-based access control |

### Writing a Hook Extension

A typical hook extension follows this pattern:

```c
/* Save previous hook */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

/* Hook implementation */
static void my_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /* Pre-processing */
    ...

    /* Call previous hook or standard implementation */
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    /* Post-processing */
    ...
}

/* Installation (in _PG_init) */
void _PG_init(void)
{
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = my_ExecutorStart;
}
```

The chaining pattern ensures multiple extensions can hook the same entry point.

---

## 8. Interaction Between Executor and Buffer Manager

**Source**: Various scan node implementations, `src/backend/storage/buffer/bufmgr.c`

### Pin/Unpin Patterns During Scan

When scanning heap pages, the executor interacts with the buffer manager through
the table access method (AM) layer. The general pattern is:

**Sequential Scan**: Pages are pinned one at a time. The previous page's buffer
pin is released before the next page is read. The `BufferHeapTupleTableSlot`
holds a pin on the buffer containing the current tuple.

```
For each page:
    1. ReadBuffer(rel, blockno)      -- pin and potentially read from disk
    2. LockBuffer(buffer, SHARE)     -- acquire shared lock
    3. For each tuple on page:
       a. Store in BufferHeapTupleTableSlot (holds pin)
       b. Return to upper node
       c. On next call, previous slot's pin is released by ExecClearTuple
    4. UnlockBuffer(buffer)          -- release lock
    5. ReleaseBuffer(buffer)         -- release pin (if slot no longer needs it)
```

**Index Scan**: Two buffers are involved -- the index page and the heap page:
1. Index page pinned during index traversal
2. For each matching TID, the heap page is pinned via `table_index_fetch_tuple()`
3. Index page pin may be held across multiple heap page fetches
4. Heap page pin transferred to the `BufferHeapTupleTableSlot`

**Bitmap Heap Scan**: Pages are fetched in TID order (sorted by block number for
sequential I/O). The TIDBitmap is built first (by BitmapIndexScan), then heap
pages are read:
1. Build TIDBitmap (index pages pinned/unpinned during scan)
2. For each page in bitmap:
   a. Pin and lock the heap page
   b. Check visibility for all matching tuples on the page
   c. Return qualifying tuples one at a time (pin held by slot)

### Buffer Pin Lifecycle with TupleTableSlot

The `BufferHeapTupleTableSlot` subtype holds a buffer pin:

```c
typedef struct BufferHeapTupleTableSlot
{
    HeapTupleTableSlot base;
    Buffer      buffer;     /* buffer holding the tuple, or InvalidBuffer */
} BufferHeapTupleTableSlot;
```

**Pin Acquisition**: `ExecStoreBufferHeapTuple()` stores a heap tuple along with
its buffer pin in the slot. The pin ensures the buffer remains in shared buffers
and the tuple pointer remains valid.

**Pin Release**: When `ExecClearTuple()` is called on a `BufferHeapTupleTableSlot`,
the `clear` method calls `ReleaseBuffer()` to drop the pin.

**Pin Transfer**: During projection, if the output slot is a `VirtualTupleTableSlot`,
attribute values are extracted from the pinned buffer into Datum/isnull arrays.
Once projection is complete, the scan slot can be cleared (releasing the pin)
because the values have been copied.

### Memory Context Interaction

The per-tuple expression context (`ResetPerTupleExprContext`) interacts with
buffer pins through a subtle protocol:

1. Scan node fetches a tuple, storing it in a `BufferHeapTupleTableSlot` (pin held)
2. Expression evaluation reads attributes from the slot
3. Results are stored in the per-tuple memory context
4. `ResetPerTupleExprContext()` frees expression results but does NOT release
   the buffer pin -- that is managed by the slot lifecycle
5. On the next iteration, `ExecClearTuple()` on the scan slot releases the pin

### Prefetching

For sequential scans, the table AM may use `PrefetchBuffer()` to issue
asynchronous read requests for upcoming pages. This allows the OS to overlap
I/O with CPU processing. The `effective_io_concurrency` GUC controls the
prefetch distance.

For bitmap heap scans, prefetching is particularly effective because the page
order is known in advance from the TIDBitmap. The
`BitmapHeapScanState.prefetch_pages` field tracks the prefetch window.
