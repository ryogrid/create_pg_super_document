# fmgr_sql

## Location
[src/backend/executor/functions.c:1029-1405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L1029-L1405)

## Overview
The primary function call manager for executing SQL-language functions, handling both scalar and set-returning functions with comprehensive execution state management, parameter substitution, snapshot handling, and result processing.

## Definition

```c
Datum
fmgr_sql(PG_FUNCTION_ARGS)
```
## Detailed Description
fmgr_sql serves as the central entry point for executing SQL functions in PostgreSQL. It manages the complete lifecycle of SQL function execution, including cache validation and initialization, parameter conversion, execution state management, snapshot handling for transaction consistency, and result processing for both scalar and set-returning functions. The function supports both lazy evaluation (returning one result at a time) and materialized evaluation (returning all results at once) for set-returning functions. It handles complex scenarios like multi-statement functions, proper cleanup through error context callbacks, and manages memory contexts to ensure proper resource cleanup.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [sql_exec_error_callback](../s/sql_exec_error_callback.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)  
  - [postquel_sub_params](../p/postquel_sub_params.md)
  - [postquel_start](../p/postquel_start.md)
  - [postquel_getnext](../p/postquel_getnext.md)
  - [postquel_end](../p/postquel_end.md)
  - [postquel_get_single_result](../p/postquel_get_single_result.md)
  - [SubTransactionIsActive](../S/SubTransactionIsActive.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [RegisterExprContextCallback](../R/RegisterExprContextCallback.md)
  - [UnregisterExprContextCallback](../U/UnregisterExprContextCallback.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
- Called from (representative examples):
  - Function manager system via fmgr_info_cxt_security

## Notes and Other Information
- Implements comprehensive error handling with sql_exec_error_callback for better error reporting
- Manages function cache validation based on transaction IDs to ensure cache consistency
- Handles both set-returning and scalar functions with different execution paths
- Supports lazy evaluation for set-returning functions, allowing suspension and resumption of execution
- Manages PostgreSQL snapshots to ensure proper transaction isolation during function execution
- Uses tuplestore for buffering results, especially important for set-returning functions
- Implements proper cleanup registration/deregistration to handle premature termination scenarios
- Handles multi-statement functions by iterating through execution states until completion or suspension
- Memory context switching ensures proper lifetime management of function-related data

## Simplified Source

```c
Datum fmgr_sql(PG_FUNCTION_ARGS)
{
    SQLFunctionCachePtr fcache;
    ErrorContextCallback sqlerrcontext;
    MemoryContext oldcontext;
    bool randomAccess;
    bool lazyEvalOK;
    bool is_first;
    bool pushed_snapshot;
    execution_state *es;
    TupleTableSlot *slot;
    Datum result;
    List *eslist;

    // Set up error callback for better error reporting
    sqlerrcontext.callback = sql_exec_error_callback;
    sqlerrcontext.arg = fcinfo->flinfo;
    sqlerrcontext.previous = error_context_stack;
    error_context_stack = &sqlerrcontext;

    // Check call context and determine execution mode
    if (fcinfo->flinfo->fn_retset) {
        ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
        // Validate set-returning function requirements
        if (!rsi || !IsA(rsi, ReturnSetInfo) ||
            (rsi->allowedModes & SFRM_ValuePerCall) == 0 ||
            (rsi->allowedModes & SFRM_Materialize) == 0)
            ereport(ERROR, /* ... */);

        randomAccess = rsi->allowedModes & SFRM_Materialize_Random;
        lazyEvalOK = !(rsi->allowedModes & SFRM_Materialize_Preferred);
    } else {
        randomAccess = false;
        lazyEvalOK = true;
    }

    // Initialize or validate function cache
    fcache = (SQLFunctionCachePtr) fcinfo->flinfo->fn_extra;
    if (fcache != NULL) {
        // Check if cache is stale
        if (fcache->lxid != MyProc->vxid.lxid ||
            !SubTransactionIsActive(fcache->subxid)) {
            fcinfo->flinfo->fn_extra = NULL;
            MemoryContextDelete(fcache->fcontext);
            fcache = NULL;
        }
    }

    if (fcache == NULL) {
        init_sql_fcache(fcinfo, PG_GET_COLLATION(), lazyEvalOK);
        fcache = (SQLFunctionCachePtr) fcinfo->flinfo->fn_extra;
    }

    // Switch to function's memory context
    oldcontext = MemoryContextSwitchTo(fcache->fcontext);

    // Find first unfinished query
    eslist = fcache->func_state;
    es = NULL;
    is_first = true;
    // ... (find unfinished execution state) ...

    // Convert parameters if starting fresh execution
    if (is_first && es && es->status == F_EXEC_START)
        postquel_sub_params(fcache, fcinfo);

    // Create tuplestore for results if needed
    if (!fcache->tstore)
        fcache->tstore = tuplestore_begin_heap(randomAccess, false, work_mem);

    // Execute function commands
    pushed_snapshot = false;
    while (es) {
        bool completed;

        if (es->status == F_EXEC_START) {
            // Handle snapshot management for non-read-only functions
            if (!fcache->readonly_func) {
                CommandCounterIncrement();
                if (!pushed_snapshot) {
                    PushActiveSnapshot(GetTransactionSnapshot());
                    pushed_snapshot = true;
                } else {
                    UpdateActiveSnapshotCommandId();
                }
            }
            postquel_start(es, fcache);
        } else if (!fcache->readonly_func && !pushed_snapshot) {
            PushActiveSnapshot(es->qd->snapshot);
            pushed_snapshot = true;
        }

        // Execute the query
        completed = postquel_getnext(es, fcache);

        // Clean up completed or non-set queries
        if (completed || !fcache->returnsSet)
            postquel_end(es);

        // Break if we have a lazy-eval result
        if (es->status != F_EXEC_DONE)
            break;

        // Move to next execution state
        es = es->next;
        // ... (advance through execution states) ...
    }

    // Process results based on function type
    if (fcache->returnsSet) {
        ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

        if (es) {
            // Lazy evaluation - return single result
            // ... (extract single result from tuplestore) ...
            rsi->isDone = ExprMultipleResult;
        } else if (fcache->lazyEval) {
            // End of lazy evaluation
            rsi->isDone = ExprEndResult;
            fcinfo->isnull = true;
            result = (Datum) 0;
        } else {
            // Materialize all results
            rsi->returnMode = SFRM_Materialize;
            rsi->setResult = fcache->tstore;
            fcache->tstore = NULL;
            fcinfo->isnull = true;
            result = (Datum) 0;
        }
    } else {
        // Scalar function - return single result or NULL
        if (fcache->junkFilter) {
            slot = fcache->junkFilter->jf_resultSlot;
            if (tuplestore_gettupleslot(fcache->tstore, true, false, slot))
                result = postquel_get_single_result(slot, fcinfo, fcache, oldcontext);
            else {
                fcinfo->isnull = true;
                result = (Datum) 0;
            }
        } else {
            // VOID function
            fcinfo->isnull = true;
            result = (Datum) 0;
        }
        tuplestore_clear(fcache->tstore);
    }

    // Clean up
    if (pushed_snapshot)
        PopActiveSnapshot();

    // Reset execution states if function completed
    if (es == NULL) {
        // ... (reset all execution states) ...
    }

    error_context_stack = sqlerrcontext.previous;
    MemoryContextSwitchTo(oldcontext);

    return result;
}
```