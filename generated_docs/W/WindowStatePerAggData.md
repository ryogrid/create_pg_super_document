# WindowStatePerAggData

## Location
src/backend/executor/nodeWindowAgg.c: 106 - 160

## Overview
WindowStatePerAggData maintains execution state and cached data for plain aggregate functions used as window functions, handling transition values, function metadata, and optimization data.

## Definition
```c
typedef struct WindowStatePerAggData
{
    /* Oids of transition functions */
    Oid         transfn_oid;
    Oid         invtransfn_oid; /* may be InvalidOid */
    Oid         finalfn_oid;    /* may be InvalidOid */

    /*
     * fmgr lookup data for transition functions --- only valid when
     * corresponding oid is not InvalidOid.  Note in particular that fn_strict
     * flags are kept here.
     */
    FmgrInfo    transfn;
    FmgrInfo    invtransfn;
    FmgrInfo    finalfn;

    int         numFinalArgs;   /* number of arguments to pass to finalfn */

    /*
     * initial value from pg_aggregate entry
     */
    Datum       initValue;
    bool        initValueIsNull;

    /*
     * cached value for current frame boundaries
     */
    Datum       resultValue;
    bool        resultValueIsNull;

    /*
     * We need the len and byval info for the agg's input, result, and
     * transition data types in order to know how to copy/delete values.
     */
    int16       inputtypeLen,
                resulttypeLen,
                transtypeLen;
    bool        inputtypeByVal,
                resulttypeByVal,
                transtypeByVal;

    int         wfuncno;        /* index of associated WindowStatePerFuncData */

    /* Context holding transition value and possibly other subsidiary data */
    MemoryContext aggcontext;   /* may be private, or winstate->aggcontext */

    /* Current transition value */
    Datum       transValue;     /* current transition value */
    bool        transValueIsNull;

    int64       transValueCount; /* number of currently-aggregated rows */

    /* Data local to eval_windowaggregates() */
    bool        restart;        /* need to restart this agg in this cycle? */
} WindowStatePerAggData;
```

## Detailed Description
WindowStatePerAggData is a specialized structure designed for handling plain aggregate functions when used as window functions. Unlike regular window functions, plain aggregates can be optimized using incremental computation techniques, especially when inverse transition functions are available. This structure maintains all the state necessary for such optimizations, including transition values, function references, and cached results.

The structure supports both regular aggregation (using transition and final functions) and optimized incremental aggregation (using inverse transition functions). It manages memory contexts for aggregate computation, caches results for efficiency, and maintains metadata about data types to ensure proper memory management. The ability to restart computation mid-cycle (via the restart flag) is crucial for handling complex window frame specifications.

## Parameters / Member Variables
- `transfn_oid`: OID of the aggregate's transition function
- `invtransfn_oid`: OID of the inverse transition function (InvalidOid if not available)
- `finalfn_oid`: OID of the final function (InvalidOid if none)
- `transfn`: Function manager info for the transition function
- `invtransfn`: Function manager info for the inverse transition function
- `finalfn`: Function manager info for the final function
- `numFinalArgs`: Number of arguments to pass to the final function
- `initValue`: Initial value for the aggregate from pg_aggregate catalog
- `initValueIsNull`: Whether the initial value is NULL
- `resultValue`: Cached result value for the current frame boundaries
- `resultValueIsNull`: Whether the cached result is NULL
- `inputtypeLen`: Length of the aggregate's input data type
- `resulttypeLen`: Length of the aggregate's result data type
- `transtypeLen`: Length of the aggregate's transition data type
- `inputtypeByVal`: Whether input type is passed by value
- `resulttypeByVal`: Whether result type is passed by value
- `transtypeByVal`: Whether transition type is passed by value
- `wfuncno`: Index of the associated WindowStatePerFuncData
- `aggcontext`: Memory context for aggregate computation
- `transValue`: Current transition state value
- `transValueIsNull`: Whether the current transition value is NULL
- `transValueCount`: Number of rows currently included in the aggregate
- `restart`: Flag indicating whether aggregation needs to restart in this cycle

## Dependencies
- Functions called/Symbols referenced:
  - initValue
- Called from (representative examples):
  - ExecInitWindowAgg
  - ExecReScanWindowAgg
  - WindowStatePerAgg (typedef)

## Notes and Other Information
- This structure is only used for plain aggregates functioning as window functions, not for specialized window functions
- The presence of inverse transition functions enables significant performance optimizations for sliding window frames
- Memory management is carefully handled through the aggcontext to prevent leaks during long-running aggregations
- The restart flag is essential for handling frame boundary changes that require complete re-aggregation
- Type information is cached to optimize memory operations during aggregate state transitions
- The structure supports both strict and non-strict aggregate functions through the FmgrInfo flags