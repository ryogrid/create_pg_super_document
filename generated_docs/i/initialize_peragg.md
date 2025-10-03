# initialize_peragg

## Location
[src/backend/executor/nodeWindowAgg.c:2748-3020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2748-L3020)

## Overview
initialize_peragg initializes per-aggregate execution state for window functions by setting up transition and final functions, memory contexts, and determining whether to use moving aggregates optimization.

## Definition
```c
static WindowStatePerAggData *initialize_peragg(WindowAggState *winstate, WindowFunc *wfunc, WindowStatePerAgg peraggstate)
```

## Detailed Description
initialize_peragg is a comprehensive initialization function for window aggregates that performs several critical tasks: 1) Analyzes the aggregate function definition to determine if moving aggregates can be safely used based on frame options, function volatility, and safety constraints, 2) Sets up appropriate transition and inverse transition functions, final functions, and their expression trees, 3) Performs permission checks to ensure the aggregate owner can execute all component functions, 4) Validates that the aggregate is suitable for window function usage (e.g., final function must be read-only), 5) Resolves polymorphic types and builds function call expressions, 6) Creates dedicated memory contexts for moving aggregates to handle their different lifecycle requirements, and 7) Initializes the aggregate state with proper initial values. The function carefully balances performance optimization through moving aggregates against correctness and safety requirements.

## Parameters / Member Variables
- `winstate`: WindowAggState containing the overall window aggregation execution state
- `wfunc`: WindowFunc structure describing the window function to initialize
- `peraggstate`: WindowStatePerAgg structure to be initialized with aggregate-specific state

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache
  - [contain_volatile_functions](../c/contain_volatile_functions.md), contain_subplans
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error
  - [resolve_aggregate_transtype](../r/resolve_aggregate_transtype.md)
  - [build_aggregate_transfn_expr](../b/build_aggregate_transfn_expr.md), build_aggregate_finalfn_expr
  - [fmgr_info](../f/fmgr_info.md), fmgr_info_set_expr
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [GetAggInitVal](../G/GetAggInitVal.md)
  - AllocSetContextCreate
- Called from (representative examples):
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md) (during window aggregate node initialization)

## Notes and Other Information
- Almost identical to nodeAgg.c implementation except DISTINCT is not supported for window functions
- Moving aggregates decision logic considers safety, frame options, volatility, and subplan presence
- Creates separate memory contexts for moving aggregates to handle different restart patterns
- Validates strictness compatibility between forward and inverse transition functions
- Performs extensive permission checking for all aggregate component functions
- Located in src/backend/executor/nodeWindowAgg.c:2748-3020

## Simplified Source

```c
static WindowStatePerAggData *
initialize_peragg(WindowAggState *winstate, WindowFunc *wfunc,
                  WindowStatePerAgg peraggstate)
{
    // Get aggregate function definition
    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(wfunc->winfnoid));
    Form_pg_aggregate aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

    // Determine if we can use moving aggregates optimization
    bool use_ma_code = false;
    if (OidIsValid(aggform->aggminvtransfn) &&  // Has inverse function
        !(winstate->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) &&  // Moving frame
        !contain_volatile_functions((Node *) wfunc) &&  // No volatile functions
        !contain_subplans((Node *) wfunc))  // No subplans
    {
        use_ma_code = true;
    }

    // Set up function OIDs based on moving aggregate decision
    if (use_ma_code)
    {
        peraggstate->transfn_oid = aggform->aggmtransfn;
        peraggstate->invtransfn_oid = aggform->aggminvtransfn;
        peraggstate->finalfn_oid = aggform->aggmfinalfn;
        // Use moving aggregate versions
    }
    else
    {
        peraggstate->transfn_oid = aggform->aggtransfn;
        peraggstate->invtransfn_oid = InvalidOid;
        peraggstate->finalfn_oid = aggform->aggfinalfn;
        // Use regular aggregate versions
    }

    // Permission checks for all component functions
    // (Check that aggregate owner can execute transition, inverse, final functions)

    // Validate final function is read-only (required for window functions)
    if (finalmodify != AGGMODIFY_READ_ONLY)
        ereport(ERROR, "aggregate function does not support use as a window function");

    // Resolve polymorphic types and build expression trees
    Oid aggtranstype = resolve_aggregate_transtype(wfunc->winfnoid, ...);
    build_aggregate_transfn_expr(..., &transfnexpr, &invtransfnexpr);

    // Set up function call info for transition and final functions
    fmgr_info(transfn_oid, &peraggstate->transfn);
    if (OidIsValid(invtransfn_oid))
        fmgr_info(invtransfn_oid, &peraggstate->invtransfn);
    if (OidIsValid(finalfn_oid))
        fmgr_info(finalfn_oid, &peraggstate->finalfn);

    // Get type information
    get_typlenbyval(wfunc->wintype, &peraggstate->resulttypeLen, &peraggstate->resulttypeByVal);
    get_typlenbyval(aggtranstype, &peraggstate->transtypeLen, &peraggstate->transtypeByVal);

    // Set up initial value
    Datum textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple, initvalAttNo, &peraggstate->initValueIsNull);
    if (!peraggstate->initValueIsNull)
        peraggstate->initValue = GetAggInitVal(textInitVal, aggtranstype);

    // Create separate memory context for moving aggregates
    if (OidIsValid(invtransfn_oid))
        peraggstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext, "WindowAgg Per Aggregate", ...);
    else
        peraggstate->aggcontext = winstate->aggcontext;

    ReleaseSysCache(aggTuple);
    return peraggstate;
}
```

This function initializes window aggregate state by:
1. **Function Analysis**: Reading aggregate definition from system catalogs
2. **Optimization Decision**: Determining if moving aggregates can be safely used
3. **Function Setup**: Configuring transition, inverse, and final functions
4. **Validation**: Ensuring aggregate is suitable for window function usage
5. **Type Resolution**: Handling polymorphic types and building expressions
6. **Memory Management**: Creating appropriate memory contexts for different aggregate types