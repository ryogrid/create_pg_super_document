# ExecInitMemoize

## Location
[src/backend/executor/nodeMemoize.c:952-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L952-L1079)

## Overview
ExecInitMemoize initializes a MemoizeState node for the Memoize executor, setting up hash functions, expression contexts, memory management, and all necessary data structures for parameter-based result caching.

## Definition

```c
MemoizeState *
ExecInitMemoize(Memoize *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitMemoize performs comprehensive initialization of a Memoize node's runtime state. It sets up the hash table infrastructure for caching, initializes expression evaluation for cache key parameters, configures memory management with appropriate limits, and establishes the LRU (Least Recently Used) eviction mechanism. The function creates specialized tuple slots for both hash key storage and result caching, builds hash and equality functions for parameter comparison, and initializes all statistics counters. It defers the actual hash table allocation until execution time to avoid unnecessary memory consumption during planning.

## Parameters / Member Variables
- `node`: The Memoize plan node containing configuration like parameter expressions, hash operators, collations, and optimization flags
- `estate`: The execution state containing the query's execution context, memory contexts, and global execution information  
- `eflags`: Execution flags that control node behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - outerPlan
  - [ExecInitNode](ExecInitNode.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md)
  - [ExecTypeFromExprList](ExecTypeFromExprList.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [get_op_hash_functions](../g/get_op_hash_functions.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [ExecInitExpr](ExecInitExpr.md)
  - [get_opcode](../g/get_opcode.md)
  - [ExecBuildParamSetEqual](ExecBuildParamSetEqual.md)
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md)
  - AllocSetContextCreate
  - [dlist_init](../d/dlist_init.md)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main node initialization dispatcher)

## Notes and Other Information
- Validates that unsupported execution flags (EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK) are not set since Memoize doesn't support backward scanning or mark/restore
- Creates separate memory context "MemoizeHashTable" for cache data to enable easy cleanup and memory tracking
- Supports both binary mode (bit-by-bit key comparison) and logical mode (using type-specific equality operators) for parameter matching
- Optimizes for single-row scenarios where cache entries can be marked complete after the first tuple
- Uses TTSOpsMinimalTuple for efficient storage and TTSOpsVirtual for temporary key operations
- Initializes comprehensive instrumentation counters for monitoring cache performance

## Simplified Source

```c
MemoizeState *
ExecInitMemoize(Memoize *node, EState *estate, int eflags)
{
    MemoizeState *mstate = makeNode(MemoizeState);
    Plan *outerNode;
    int i, nkeys;
    Oid *eqfuncoids;

    // Validate execution flags - backward scan and mark/restore not supported
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Initialize state structure
    mstate->ss.ps.plan = (Plan *) node;
    mstate->ss.ps.state = estate;
    mstate->ss.ps.ExecProcNode = ExecMemoize;

    // Set up expression context
    ExecAssignExprContext(estate, &mstate->ss.ps);

    // Initialize child node
    outerNode = outerPlan(node);
    outerPlanState(mstate) = ExecInitNode(outerNode, estate, eflags);

    // Initialize result and scan slots
    ExecInitResultTupleSlotTL(&mstate->ss.ps, &TTSOpsMinimalTuple);
    mstate->ss.ps.ps_ProjInfo = NULL; // No projection needed
    ExecCreateScanSlotFromOuterPlan(estate, &mstate->ss, &TTSOpsMinimalTuple);

    // Set initial cache state
    mstate->mstatus = MEMO_CACHE_LOOKUP;

    // Set up hash key infrastructure
    mstate->nkeys = nkeys = node->numKeys;
    mstate->hashkeydesc = ExecTypeFromExprList(node->param_exprs);
    mstate->tableslot = MakeSingleTupleTableSlot(mstate->hashkeydesc, &TTSOpsMinimalTuple);
    mstate->probeslot = MakeSingleTupleTableSlot(mstate->hashkeydesc, &TTSOpsVirtual);

    // Initialize parameter expressions and hash functions
    mstate->param_exprs = (ExprState **) palloc(nkeys * sizeof(ExprState *));
    mstate->collations = node->collations;
    mstate->hashfunctions = (FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
    eqfuncoids = palloc(nkeys * sizeof(Oid));

    // Set up hash and equality functions for each key
    for (i = 0; i < nkeys; i++)
    {
        Oid hashop = node->hashOperators[i];
        Oid left_hashfn, right_hashfn;
        Expr *param_expr = (Expr *) list_nth(node->param_exprs, i);

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
            elog(ERROR, "could not find hash function for hash operator %u", hashop);

        fmgr_info(left_hashfn, &mstate->hashfunctions[i]);
        mstate->param_exprs[i] = ExecInitExpr(param_expr, (PlanState *) mstate);
        eqfuncoids[i] = get_opcode(hashop);
    }

    // Build cache equality expression
    mstate->cache_eq_expr = ExecBuildParamSetEqual(mstate->hashkeydesc,
                                                  &TTSOpsMinimalTuple,
                                                  &TTSOpsVirtual,
                                                  eqfuncoids,
                                                  node->collations,
                                                  node->param_exprs,
                                                  (PlanState *) mstate);

    pfree(eqfuncoids);

    // Initialize memory management
    mstate->mem_used = 0;
    mstate->mem_limit = get_hash_memory_limit();
    mstate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
                                                "MemoizeHashTable",
                                                ALLOCSET_DEFAULT_SIZES);

    // Initialize LRU list and state variables
    dlist_init(&mstate->lru_list);
    mstate->last_tuple = NULL;
    mstate->entry = NULL;

    // Set optimization flags
    mstate->singlerow = node->singlerow;
    mstate->keyparamids = node->keyparamids;
    mstate->binary_mode = node->binary_mode;

    // Initialize statistics and defer hash table creation
    memset(&mstate->stats, 0, sizeof(MemoizeInstrumentation));
    mstate->hashtable = NULL;

    return mstate;
}
```