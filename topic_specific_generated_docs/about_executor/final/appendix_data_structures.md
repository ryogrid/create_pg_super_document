# Appendix D: Key Data Structures

**PostgreSQL 17.6 Executor Subsystem**

This appendix documents the essential struct definitions that form the executor's
runtime data model. All definitions are from the PostgreSQL 17.6 source tree.

---

## Table of Contents

1. [PlanState](#1-planstate)
2. [TupleTableSlot](#2-tupletableslot)
3. [ExprState](#3-exprstate)
4. [ExprContext](#4-exprcontext)
5. [EState](#5-estate)
6. [QueryDesc](#6-querydesc)
7. [AggState](#7-aggstate)
8. [HashJoinState](#8-hashjoinstate)
9. [ScanState and JoinState](#9-scanstate-and-joinstate)
10. [TupleTableSlotOps](#10-tupletableslotops)

---

## 1. PlanState

**Source**: `src/include/nodes/execnodes.h:1113`

The base struct for all executor node state. Every specific node state (e.g.,
`SeqScanState`, `HashJoinState`) embeds `PlanState` as its first member.

```c
typedef struct PlanState
{
    NodeTag     type;

    Plan       *plan;                   /* associated Plan node */
    EState     *state;                  /* shared per-query EState */

    ExecProcNodeMtd ExecProcNode;       /* function to return next tuple */
    ExecProcNodeMtd ExecProcNodeReal;   /* actual function (if above is wrapper) */

    Instrumentation *instrument;        /* optional runtime stats */
    WorkerInstrumentation *worker_instrument;  /* per-worker stats */
    struct SharedJitInstrumentation *worker_jit_instrument;

    /* Structural links (parallel the Plan tree) */
    ExprState  *qual;                   /* boolean qual condition */
    struct PlanState *lefttree;         /* outer (left) input */
    struct PlanState *righttree;        /* inner (right) input */

    List       *initPlan;               /* init SubPlanState nodes */
    List       *subPlan;                /* correlated SubPlanState nodes */

    /* Parameter-change-driven rescanning */
    Bitmapset  *chgParam;               /* IDs of changed Params */

    /* Result tuple management */
    TupleDesc   ps_ResultTupleDesc;     /* output tuple descriptor */
    TupleTableSlot *ps_ResultTupleSlot; /* slot for result tuples */
    ExprContext *ps_ExprContext;         /* expression evaluation context */
    ProjectionInfo *ps_ProjInfo;        /* projection info, or NULL */

    bool        async_capable;          /* true if async-capable */
    TupleDesc   scandesc;               /* scanslot descriptor (optimization) */

    /* Slot type info for JIT optimization */
    const TupleTableSlotOps *scanops;
    const TupleTableSlotOps *innerops;
    const TupleTableSlotOps *outerops;
    const TupleTableSlotOps *resultops;
    bool        scanopsfixed;
    bool        inneropsfixed;
    bool        outeropsfixed;
    bool        resultopsfixed;
    bool        scanopsset;
    bool        inneropsset;
    bool        outeropsset;
    bool        resultopsset;
} PlanState;
```

### Key Fields

| Field | Purpose |
|-------|---------|
| `ExecProcNode` | Function pointer called by the Volcano iterator. Set during `ExecInitNode`. |
| `qual` | Compiled qualification expression. Evaluated by `ExecQual()` for filtering. |
| `lefttree` / `righttree` | Links to child state nodes (outer/inner). |
| `ps_ProjInfo` | If non-NULL, `ExecProject()` uses this to compute output tuples. If NULL, tuples pass through without projection. |
| `chgParam` | Set by parent nodes to signal parameter changes, triggering rescan. |
| `ps_ExprContext` | Provides tuple slots and memory contexts for expression evaluation. |

---

## 2. TupleTableSlot

**Source**: `src/include/executor/tuptable.h:114`

The universal tuple container. All tuples flowing through the executor pass
through `TupleTableSlot` instances.

```c
typedef struct TupleTableSlot
{
    NodeTag     type;
    uint16      tts_flags;              /* TTS_FLAG_EMPTY, etc. */
    AttrNumber  tts_nvalid;             /* # of valid values in tts_values */
    const TupleTableSlotOps *const tts_ops;  /* virtual method table */
    TupleDesc   tts_tupleDescriptor;    /* tuple descriptor */
    Datum      *tts_values;             /* per-attribute values */
    bool       *tts_isnull;             /* per-attribute null flags */
    MemoryContext tts_mcxt;             /* memory context of slot */
    ItemPointerData tts_tid;            /* stored tuple's TID */
    Oid         tts_tableOid;           /* table OID of tuple */
} TupleTableSlot;
```

### Subtypes

```c
typedef struct VirtualTupleTableSlot
{
    TupleTableSlot base;
    char       *data;                   /* optional allocated data area */
} VirtualTupleTableSlot;

typedef struct HeapTupleTableSlot
{
    TupleTableSlot base;
    HeapTuple   tuple;                  /* physical tuple, or NULL */
    uint32      off;                    /* deforming offset */
    HeapTupleData tupdata;              /* inline tuple header */
} HeapTupleTableSlot;

typedef struct BufferHeapTupleTableSlot
{
    HeapTupleTableSlot base;
    Buffer      buffer;                 /* pinned buffer, or InvalidBuffer */
} BufferHeapTupleTableSlot;

typedef struct MinimalTupleTableSlot
{
    TupleTableSlot base;
    HeapTuple   tuple;                  /* tuple with minimal header */
    MinimalTuple mintuple;              /* actual MinimalTuple */
    HeapTupleData minhdr;               /* header for heap routines */
    uint32      off;                    /* deforming offset */
} MinimalTupleTableSlot;
```

### Flag Constants

| Flag | Value | Meaning |
|------|-------|---------|
| `TTS_FLAG_EMPTY` | `(1 << 1)` | Slot contains no tuple |
| `TTS_FLAG_SHOULDFREE` | `(1 << 2)` | Tuple should be pfree'd on clear |
| `TTS_FLAG_SLOW` | `(1 << 3)` | Cannot use fast deforming path |
| `TTS_FLAG_FIXED` | `(1 << 4)` | Descriptor is fixed, not a reference |

---

## 3. ExprState

**Source**: `src/include/nodes/execnodes.h:78`

The compiled representation of an SQL expression, ready for evaluation.

```c
typedef struct ExprState
{
    NodeTag     type;
    uint8       flags;                  /* EEO_FLAG_IS_QUAL, etc. */
    bool        resnull;                /* result null flag */
    Datum       resvalue;               /* result value */
    TupleTableSlot *resultslot;         /* result slot (projections) */
    struct ExprEvalStep *steps;         /* compiled step array */
    ExprStateEvalFunc evalfunc;         /* evaluation function pointer */
    Expr       *expr;                   /* original expression (debug) */
    void       *evalfunc_private;       /* private state for evalfunc */
    int         steps_len;              /* number of steps */
    int         steps_alloc;            /* allocated step array length */
    struct PlanState *parent;           /* parent PlanState, if any */
    ParamListInfo ext_params;           /* for PARAM_EXTERN nodes */
} ExprState;
```

### Evaluation Function Dispatch

The `evalfunc` pointer is set to one of:

| Function | When Used |
|----------|-----------|
| `ExecInterpExpr` | Default: step-by-step interpreter |
| JIT-compiled function | When JIT is enabled and cost thresholds are met |
| `ExecJustConst` | Expression is a single constant |
| `ExecJustInnerVar` | Expression is a single inner-tuple attribute |
| `ExecJustOuterVar` | Expression is a single outer-tuple attribute |
| `ExecJustScanVar` | Expression is a single scan-tuple attribute |
| `ExecJustAssignInnerVar` | Assignment of a single inner-tuple attribute |

---

## 4. ExprContext

**Source**: `src/include/nodes/execnodes.h:251`

The runtime context for expression evaluation. Provides access to current tuples
and memory management.

```c
typedef struct ExprContext
{
    NodeTag     type;

    /* Tuple slots for Var node resolution */
    TupleTableSlot *ecxt_scantuple;     /* current scan tuple */
    TupleTableSlot *ecxt_innertuple;    /* current inner tuple */
    TupleTableSlot *ecxt_outertuple;    /* current outer tuple */

    /* Memory contexts */
    MemoryContext ecxt_per_query_memory; /* lives for entire query */
    MemoryContext ecxt_per_tuple_memory; /* reset each tuple */

    /* Parameter values */
    ParamExecData *ecxt_param_exec_vals;  /* PARAM_EXEC params */
    ParamListInfo ecxt_param_list_info;   /* external params */

    /* Aggregate/window function values */
    Datum      *ecxt_aggvalues;         /* precomputed agg values */
    bool       *ecxt_aggnulls;          /* null flags for aggs */

    /* CASE expression support */
    Datum       caseValue_datum;
    bool        caseValue_isNull;

    /* Domain constraint support */
    Datum       domainValue_datum;
    bool        domainValue_isNull;

    /* Link to parent EState */
    struct EState *ecxt_estate;

    /* Shutdown/rescan callbacks */
    ExprContext_CB *ecxt_callbacks;
} ExprContext;
```

### Memory Context Usage

| Context | Lifetime | Reset Frequency | Contents |
|---------|----------|-----------------|----------|
| `ecxt_per_query_memory` | Entire query | Never (during query) | ExprState, compiled steps |
| `ecxt_per_tuple_memory` | Per-tuple | Every `ResetExprContext()` | Intermediate expression results |

---

## 5. EState

**Source**: `src/include/nodes/execnodes.h:621`

Per-query execution state shared by all plan nodes.

```c
typedef struct EState
{
    NodeTag     type;

    /* Scan state */
    ScanDirection es_direction;         /* current scan direction */

    /* MVCC */
    Snapshot    es_snapshot;            /* time qual for tuple visibility */
    Snapshot    es_crosscheck_snapshot; /* RI crosscheck snapshot */

    /* Range table */
    List       *es_range_table;         /* List of RangeTblEntry */
    Index       es_range_table_size;    /* size of range table arrays */
    Relation   *es_relations;           /* per-RTE Relation pointers */
    struct ExecRowMark **es_rowmarks;   /* per-RTE ExecRowMarks */
    List       *es_rteperminfos;        /* RTEPermissionInfo list */
    PlannedStmt *es_plannedstmt;        /* link to planned statement */
    const char *es_sourceText;          /* query source text */

    /* Result filtering */
    JunkFilter *es_junkFilter;          /* top-level junk filter */
    CommandId   es_output_cid;          /* CID for INSERT/UPDATE/DELETE */

    /* DML target tables */
    ResultRelInfo **es_result_relations;
    List       *es_opened_result_relations;
    PartitionDirectory es_partition_directory;
    List       *es_tuple_routing_result_relations;

    /* Triggers */
    List       *es_trig_target_relations;

    /* Parameters */
    ParamListInfo es_param_list_info;   /* external param values */
    ParamExecData *es_param_exec_vals;  /* internal param values */

    QueryEnvironment *es_queryEnv;

    /* Memory and bookkeeping */
    MemoryContext es_query_cxt;         /* per-query memory context */
    List       *es_tupleTable;          /* all TupleTableSlots */
    uint64      es_processed;           /* tuples processed this ExecutorRun */
    uint64      es_total_processed;     /* total across all ExecutorRun calls */

    /* Execution control */
    int         es_top_eflags;          /* eflags from ExecutorStart */
    int         es_instrument;          /* InstrumentOption flags */
    bool        es_finished;            /* true after ExecutorFinish */

    /* Substructures */
    List       *es_exprcontexts;        /* ExprContexts within EState */
    List       *es_subplanstates;       /* PlanStates for SubPlans */
    List       *es_auxmodifytables;     /* secondary ModifyTableStates */

    /* Per-tuple expression context */
    ExprContext *es_per_tuple_exprcontext;

    /* EPQ support */
    struct EPQState *es_epq_active;

    /* Parallel execution */
    bool        es_use_parallel_mode;

    /* JIT */
    int         es_jit_flags;
    struct JitContext *es_jit;
    struct JitInstrumentation *es_jit_worker_instr;
} EState;
```

---

## 6. QueryDesc

**Source**: `src/include/executor/execdesc.h:33`

The top-level descriptor carrying everything the executor needs.

```c
typedef struct QueryDesc
{
    CmdType     operation;              /* CMD_SELECT, CMD_UPDATE, etc. */
    PlannedStmt *plannedstmt;           /* planner output */
    const char *sourceText;             /* query source text */
    Snapshot    snapshot;               /* MVCC snapshot */
    Snapshot    crosscheck_snapshot;    /* RI crosscheck snapshot */
    DestReceiver *dest;                 /* tuple output destination */
    ParamListInfo params;               /* parameter values */
    QueryEnvironment *queryEnv;         /* query environment */
    int         instrument_options;     /* InstrumentOption flags */

    /* Set by ExecutorStart */
    TupleDesc   tupDesc;                /* result tuple descriptor */
    EState     *estate;                 /* per-query execution state */
    PlanState  *planstate;              /* root of PlanState tree */

    /* Set by ExecutePlan */
    bool        already_executed;       /* true if previously executed */

    /* Plugins may set */
    struct Instrumentation *totaltime;  /* total ExecutorRun time */
} QueryDesc;
```

---

## 7. AggState

**Source**: `src/include/nodes/execnodes.h` (simplified)

Runtime state for the Agg plan node.

```c
typedef struct AggState
{
    ScanState   ss;                     /* base (reads from child plan) */
    List       *aggs;                   /* list of Aggref nodes */
    int         numaggs;                /* count of aggregate functions */
    int         numtrans;               /* count of transition states */
    AggStrategy aggstrategy;            /* AGG_PLAIN/SORTED/HASHED/MIXED */
    AggSplit    aggsplit;               /* AGGSPLIT_INITIAL_SERIAL, etc. */
    AggStatePerPhase phase;             /* current phase pointer */
    int         numphases;              /* number of phases (GROUPING SETS) */
    int         current_phase;          /* current phase index */
    AggStatePerAgg peragg;              /* per-aggregate state array */
    AggStatePerTrans pertrans;          /* per-transition state array */
    ExprContext *hashcontext;           /* econtext for hash operations */
    ExprContext **aggcontexts;          /* per-grouping-set contexts */
    ExprContext *tmpcontext;            /* short-lived context */
    ExprContext *curaggcontext;         /* current aggregate context */
    AggStatePerAgg curperagg;           /* currently active per-agg */
    AggStatePerTrans curpertrans;       /* currently active per-trans */
    bool        input_done;             /* true if input exhausted */
    bool        agg_done;               /* true if all output emitted */
    int         projected_set;          /* last projected grouping set */
    int         current_set;            /* current grouping set */
    Bitmapset  *grouped_cols;           /* grouped column positions */
    List       *all_grouped_cols;       /* list of grouped column sets */
    /* Hash aggregation */
    int         maxsets;                /* max grouping sets */
    AggStatePerHash perhash;            /* per-hash-table state */
    AggStatePerGroup *hash_pergroup;    /* per-group state in hash */
    TupleHashTable *hashtable;          /* hash tables */
    bool        table_filled;           /* hash table populated? */
    int         hash_ngroups_current;   /* current group count */
    /* Spill management */
    HashAggSpill *hash_spills;          /* spill partitions */
    TupleTableSlot *hash_spill_rslot;   /* slot for spill reading */
    TupleTableSlot *hash_spill_wslot;   /* slot for spill writing */
    /* ... additional fields ... */
} AggState;
```

---

## 8. HashJoinState

**Source**: `src/include/nodes/execnodes.h` (simplified)

Runtime state for the HashJoin plan node.

```c
typedef struct HashJoinState
{
    JoinState   js;                     /* base join state */
    ExprState  *hashclauses;            /* hash join clauses */
    List       *hj_OuterHashKeys;       /* outer hash key expressions */
    List       *hj_HashOperators;       /* hash operator OIDs */
    List       *hj_Collations;          /* collations for hash ops */
    HashJoinTable hj_HashTable;         /* the hash table */
    uint32      hj_CurHashValue;        /* hash value of current outer */
    int         hj_CurBucketNo;         /* current bucket number */
    int         hj_CurSkewBucketNo;     /* skew bucket number */
    HashJoinTuple hj_CurTuple;          /* current match in bucket chain */
    TupleTableSlot *hj_OuterTupleSlot;  /* outer tuple slot */
    TupleTableSlot *hj_HashTupleSlot;   /* inner (hash) tuple slot */
    TupleTableSlot *hj_NullOuterTupleSlot; /* for RIGHT/FULL null-fill */
    TupleTableSlot *hj_NullInnerTupleSlot; /* for LEFT/ANTI null-fill */
    TupleTableSlot *hj_FirstOuterTupleSlot; /* stashed first outer */
    int         hj_JoinState;           /* current HJ_* state */
    bool        hj_MatchedOuter;        /* current outer has match */
    bool        hj_OuterNotEmpty;       /* outer is not empty */
} HashJoinState;
```

### Hash Join State Machine Constants

```c
#define HJ_BUILD_HASHTABLE     1    /* Build phase */
#define HJ_NEED_NEW_OUTER      2    /* Fetch next outer tuple */
#define HJ_SCAN_BUCKET         3    /* Probe hash bucket */
#define HJ_FILL_OUTER_TUPLE    4    /* Emit null-fill for LEFT/ANTI */
#define HJ_FILL_INNER_TUPLES   5    /* Emit unmatched inner (RIGHT/FULL) */
#define HJ_NEED_NEW_BATCH      6    /* Advance to next batch */
```

---

## 9. ScanState and JoinState

**Source**: `src/include/nodes/execnodes.h`

### ScanState

Base struct for all scan nodes.

```c
typedef struct ScanState
{
    PlanState   ps;                     /* base plan state */
    Relation    ss_currentRelation;     /* relation being scanned */
    struct TableScanDescData *ss_currentScanDesc; /* scan descriptor */
    TupleTableSlot *ss_ScanTupleSlot;   /* slot for scan tuples */
} ScanState;
```

### JoinState

Base struct for all join nodes.

```c
typedef struct JoinState
{
    PlanState   ps;                     /* base plan state */
    JoinType    jointype;               /* JOIN_INNER, JOIN_LEFT, etc. */
    bool        single_match;           /* semi-join or inner_unique */
    ExprState  *joinqual;               /* join qualification */
} JoinState;
```

### MergeJoinState

```c
typedef struct MergeJoinState
{
    JoinState   js;                     /* base join state */
    int         mj_NumClauses;          /* number of merge clauses */
    MergeJoinClause mj_Clauses;         /* merge clause array */
    int         mj_JoinState;           /* current EXEC_MJ_* state */
    bool        mj_MatchedOuter;        /* current outer has match */
    bool        mj_MatchedInner;        /* current inner has match */
    TupleTableSlot *mj_OuterTupleSlot;  /* current outer tuple */
    TupleTableSlot *mj_InnerTupleSlot;  /* current inner tuple */
    TupleTableSlot *mj_MarkedTupleSlot; /* marked position for restore */
    TupleTableSlot *mj_NullOuterTupleSlot; /* RIGHT/FULL null-fill */
    TupleTableSlot *mj_NullInnerTupleSlot; /* LEFT/FULL null-fill */
    bool        mj_FillOuter;           /* LEFT or FULL join */
    bool        mj_FillInner;           /* RIGHT or FULL join */
    bool        mj_ExtraMarks;          /* extra mark/restore needed */
} MergeJoinState;
```

### NestLoopState

```c
typedef struct NestLoopState
{
    JoinState   js;                     /* base join state */
    bool        nl_NeedNewOuter;        /* need next outer tuple */
    bool        nl_MatchedOuter;        /* current outer has match */
    TupleTableSlot *nl_NullInnerTupleSlot; /* LEFT/ANTI null-fill */
} NestLoopState;
```

---

## 10. TupleTableSlotOps

**Source**: `src/include/executor/tuptable.h:134`

The virtual method table for slot operations.

```c
struct TupleTableSlotOps
{
    size_t      base_slot_size;         /* minimum size of the slot */

    /* Initialization and cleanup */
    void        (*init) (TupleTableSlot *slot);
    void        (*release) (TupleTableSlot *slot);

    /* Core operations */
    void        (*clear) (TupleTableSlot *slot);
    void        (*getsomeattrs) (TupleTableSlot *slot, int natts);
    Datum       (*getsysattr) (TupleTableSlot *slot, int attnum, bool *isnull);
    bool        (*is_current_xact_tuple) (TupleTableSlot *slot);
    void        (*materialize) (TupleTableSlot *slot);
    void        (*copyslot) (TupleTableSlot *dstslot, TupleTableSlot *srcslot);

    /* Tuple extraction */
    HeapTuple   (*get_heap_tuple) (TupleTableSlot *slot);
    MinimalTuple (*get_minimal_tuple) (TupleTableSlot *slot);

    /* Tuple copying */
    HeapTuple   (*copy_heap_tuple) (TupleTableSlot *slot);
    MinimalTuple (*copy_minimal_tuple) (TupleTableSlot *slot);
};
```

### Predefined Slot Types

| Global | Slot Subtype | Use Case |
|--------|-------------|----------|
| `TTSOpsVirtual` | `VirtualTupleTableSlot` | Projected result tuples, constants |
| `TTSOpsHeapTuple` | `HeapTupleTableSlot` | Heap tuples not in a buffer |
| `TTSOpsMinimalTuple` | `MinimalTupleTableSlot` | Hash table entries, tuplestores |
| `TTSOpsBufferHeapTuple` | `BufferHeapTupleTableSlot` | Heap tuples in shared buffers (holds pin) |
