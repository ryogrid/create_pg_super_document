# ExecMakeTableFunctionResult

## Location
[src/backend/executor/execSRF.c:101-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L101-L443)

## Overview
Evaluates a table function and materializes its results in a Tuplestore object, handling both set-returning and scalar functions with proper memory management and protocol validation.

## Definition
```c
Tuplestorestate *ExecMakeTableFunctionResult(SetExprState *setexpr, ExprContext *econtext, MemoryContext argContext, TupleDesc expectedDesc, bool randomAccess)
```

## Detailed Description
ExecMakeTableFunctionResult is the main execution function for table functions in FROM clauses. It handles the complete lifecycle of function execution, from argument evaluation to result materialization. The function supports multiple execution modes:

1. **ValuePerCall Protocol**: Functions that return one tuple at a time through repeated calls, with the function maintaining state between calls
2. **Materialize Protocol**: Functions that return all results at once in a pre-built tuplestore
3. **Generic Expression Path**: Fallback for optimized expressions that don't use the standard function call interface

Key responsibilities include:
- Memory context management to prevent leaks during long-running operations  
- Type consistency validation for RECORD-returning functions
- Proper handling of NULL results and empty result sets
- Protocol compliance checking for set-returning functions
- Tuple descriptor creation and matching for both scalar and composite return types

The function creates a tuplestore to hold all results and handles the complex interaction between PostgreSQL's function call protocols and the executor's tuple processing requirements.

## Parameters / Member Variables
- `setexpr`: SetExprState containing the prepared function call information and execution state
- `econtext`: Expression context providing memory contexts and execution environment
- `argContext`: Separate memory context for function arguments to avoid leaks across calls
- `expectedDesc`: Expected tuple descriptor for the function result, used for type validation
- `randomAccess`: Whether the tuplestore should support random access (affects storage strategy)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md), MemoryContextSwitchTo (memory management)
  - [type_is_rowtype](../t/type_is_rowtype.md) (type checking)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md), tuplestore_puttuple, tuplestore_putvalues (result storage)
  - InitFunctionCallInfoData, FunctionCallInvoke (function call infrastructure)
  - [ExecEvalFuncArgs](ExecEvalFuncArgs.md), ExecEvalExpr (argument and expression evaluation)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md), TupleDescInitEntry (tuple descriptor creation)
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md), tupledesc_match (type descriptor handling)
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md), pgstat_end_function_usage (statistics)
- Called from (representative examples):
  - [FunctionNext](../F/FunctionNext.md) (src/backend/executor/nodeFunctionscan.c:94, 153)

## Notes and Other Information
- Implements careful memory management with separate contexts for arguments, per-tuple operations, and query-lifetime objects
- Validates that set-returning functions follow the correct protocol (ValuePerCall or Materialize modes)
- Handles type consistency checking for RECORD types to ensure all returned rows have the same structure
- Supports both tuple-returning and scalar functions, creating appropriate tuple descriptors for each case
- For NULL results from tuple-returning functions, expands them to rows of all NULLs using the expected descriptor
- Includes comprehensive error handling for protocol violations and type mismatches
- Uses ReturnSetInfo structure to communicate with set-returning functions about expected behavior and capabilities

## Simplified Source

```c
Tuplestorestate *
ExecMakeTableFunctionResult(SetExprState *setexpr,
                           ExprContext *econtext,
                           MemoryContext argContext,
                           TupleDesc expectedDesc,
                           bool randomAccess)
{
    Tuplestorestate *tupstore = NULL;
    TupleDesc tupdesc = NULL;
    Oid funcrettype;
    bool returnsTuple;
    bool returnsSet = false;
    FunctionCallInfo fcinfo;
    ReturnSetInfo rsinfo;
    HeapTupleData tmptup;
    MemoryContext callerContext;
    bool first_time = true;

    // Setup memory contexts
    MemoryContextReset(argContext);
    callerContext = MemoryContextSwitchTo(argContext);

    funcrettype = exprType((Node *) setexpr->expr);
    returnsTuple = type_is_rowtype(funcrettype);

    // Setup return set info for communication with function
    rsinfo.type = T_ReturnSetInfo;
    rsinfo.econtext = econtext;
    rsinfo.expectedDesc = expectedDesc;
    rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred);
    if (randomAccess)
        rsinfo.allowedModes |= (int) SFRM_Materialize_Random;
    rsinfo.returnMode = SFRM_ValuePerCall;
    rsinfo.setResult = NULL;
    rsinfo.setDesc = NULL;

    fcinfo = palloc(SizeForFunctionCallInfo(list_length(setexpr->args)));

    // Handle regular function vs optimized expression
    if (!setexpr->elidedFuncState) {
        returnsSet = setexpr->funcReturnsSet;
        InitFunctionCallInfoData(*fcinfo, &(setexpr->func),
                                list_length(setexpr->args),
                                setexpr->fcinfo->fncollation,
                                NULL, (Node *) &rsinfo);

        // Evaluate function arguments
        ExecEvalFuncArgs(fcinfo, setexpr->args, econtext);

        // Handle strict functions with NULL arguments
        if (setexpr->func.fn_strict) {
            for (int i = 0; i < fcinfo->nargs; i++) {
                if (fcinfo->args[i].isnull)
                    goto no_function_result;
            }
        }
    } else {
        InitFunctionCallInfoData(*fcinfo, NULL, 0, InvalidOid, NULL, NULL);
    }

    // Switch to per-tuple context for function execution
    MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    // Main execution loop
    for (;;) {
        Datum result;

        CHECK_FOR_INTERRUPTS();
        ResetExprContext(econtext);

        // Call function or evaluate expression
        if (!setexpr->elidedFuncState) {
            fcinfo->isnull = false;
            rsinfo.isDone = ExprSingleResult;
            result = FunctionCallInvoke(fcinfo);
        } else {
            result = ExecEvalExpr(setexpr->elidedFuncState, econtext, &fcinfo->isnull);
            rsinfo.isDone = ExprSingleResult;
        }

        // Handle ValuePerCall protocol
        if (rsinfo.returnMode == SFRM_ValuePerCall) {
            if (rsinfo.isDone == ExprEndResult)
                break;

            // Initialize tuplestore on first result
            if (first_time) {
                MemoryContext oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
                tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
                rsinfo.setResult = tupstore;

                if (!returnsTuple) {
                    tupdesc = CreateTemplateTupleDesc(1);
                    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "column",
                                     funcrettype, -1, 0);
                    rsinfo.setDesc = tupdesc;
                }
                MemoryContextSwitchTo(oldcontext);
            }

            // Store result in tuplestore
            if (returnsTuple) {
                if (!fcinfo->isnull) {
                    HeapTupleHeader td = DatumGetHeapTupleHeader(result);

                    // Handle tuple descriptor for first non-NULL result
                    if (tupdesc == NULL) {
                        MemoryContext oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
                        tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td),
                                                            HeapTupleHeaderGetTypMod(td));
                        rsinfo.setDesc = tupdesc;
                        MemoryContextSwitchTo(oldcontext);
                    }

                    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                    tmptup.t_data = td;
                    tuplestore_puttuple(tupstore, &tmptup);
                } else {
                    // NULL tuple result - expand to all-nulls row
                    int natts = expectedDesc->natts;
                    bool *nullflags = (bool *) palloc(natts * sizeof(bool));
                    memset(nullflags, true, natts * sizeof(bool));
                    tuplestore_putvalues(tupstore, expectedDesc, NULL, nullflags);
                }
            } else {
                // Scalar result
                tuplestore_putvalues(tupstore, tupdesc, &result, &fcinfo->isnull);
            }

            if (rsinfo.isDone != ExprMultipleResult)
                break;
        }
        else if (rsinfo.returnMode == SFRM_Materialize) {
            // Function materialized results itself
            break;
        }

        first_time = false;
    }

no_function_result:
    // Create empty tuplestore if no results
    if (rsinfo.setResult == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
        tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
        rsinfo.setResult = tupstore;
        MemoryContextSwitchTo(oldcontext);

        if (!returnsSet) {
            int natts = expectedDesc->natts;
            bool *nullflags = (bool *) palloc(natts * sizeof(bool));
            memset(nullflags, true, natts * sizeof(bool));
            tuplestore_putvalues(tupstore, expectedDesc, NULL, nullflags);
        }
    }

    // Validate tuple descriptor if provided
    if (rsinfo.setDesc) {
        tupledesc_match(expectedDesc, rsinfo.setDesc);
        if (rsinfo.setDesc->tdrefcount == -1)
            FreeTupleDesc(rsinfo.setDesc);
    }

    MemoryContextSwitchTo(callerContext);
    return rsinfo.setResult;
}
```