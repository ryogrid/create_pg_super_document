# multirange_agg_transfn

## Location
[src/backend/utils/adt/multirangetypes.c:1412-1464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1412-L1464)

## Overview
The transition function for the multirange_agg aggregate that decomposes input multiranges into individual ranges and collects them for later processing by the finalize function.

## Definition
Datum multirange_agg_transfn(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the transition function for the multirange_agg aggregate operation in PostgreSQL. Unlike range_agg_transfn which works with individual ranges, this function handles multirange inputs by decomposing each multirange into its constituent ranges and accumulating all individual ranges in an ArrayBuildState.

The function validates that it's being called in a proper aggregate context and ensures the input is actually a multirange type. For each non-null multirange input, it deserializes the multirange to extract individual ranges and adds each range to the accumulation array. Special handling is provided for empty multiranges to ensure an empty result rather than a null result.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call interface containing:
  - Argument 0: Current aggregate state (ArrayBuildState pointer, nullable on first call)
  - Argument 1: New multirange value to decompose and add to the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [type_is_multirange](../t/type_is_multirange.md)
  - [multirange_get_typcache](multirange_get_typcache.md)
  - [initArrayResult](../i/initArrayResult.md)
  - PG_GETARG_MULTIRANGE_P
  - [multirange_deserialize](multirange_deserialize.md)
  - [accumArrayResult](../a/accumArrayResult.md)
  - [make_empty_range](make_empty_range.md)
  - [RangeTypePGetDatum](../R/RangeTypePGetDatum.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
  - elog
- Called from:
  - No direct callers found (called through SQL aggregate function interface)

## Notes and Other Information
- Part of PostgreSQL's multirange aggregation system
- Decomposes multiranges into individual ranges for accumulation
- Validates aggregate context and multirange type inputs
- Skips NULL input values
- Handles empty multiranges specially by adding an empty range to maintain proper semantics
- Memory allocation occurs in the aggregate context for proper cleanup
- Works in conjunction with a finalize function to implement the complete multirange_agg aggregate
- Accumulates individual ranges from all input multiranges for later merging
- Located in src/backend/utils/adt/multirangetypes.c:1412-1464

## Simplified Source

```c
Datum multirange_agg_transfn(PG_FUNCTION_ARGS) {
    MemoryContext aggContext;
    Oid multirangeTypeOid;
    TypeCacheEntry *typcache;
    TypeCacheEntry *rangeTypcache;
    ArrayBuildState *state;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, &aggContext))
        elog(ERROR, "multirange_agg_transfn called in non-aggregate context");

    // Validate input is multirange type
    multirangeTypeOid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (!type_is_multirange(multirangeTypeOid))
        elog(ERROR, "range_agg must be called with a multirange");

    // Get type cache for range operations
    typcache = multirange_get_typcache(fcinfo, multirangeTypeOid);
    rangeTypcache = typcache->rngtype;

    // Initialize or get existing state
    if (PG_ARGISNULL(0))
        state = initArrayResult(rangeTypcache->type_id, aggContext, false);
    else
        state = (ArrayBuildState *) PG_GETARG_POINTER(0);

    // Process non-null multirange input
    if (!PG_ARGISNULL(1)) {
        MultirangeType *current = PG_GETARG_MULTIRANGE_P(1);
        int32 rangeCount;
        RangeType **ranges;

        // Decompose multirange into individual ranges
        multirange_deserialize(rangeTypcache, current, &rangeCount, &ranges);

        if (rangeCount == 0) {
            // Add empty range for empty multirange
            accumArrayResult(state,
                           RangeTypePGetDatum(make_empty_range(rangeTypcache)),
                           false, rangeTypcache->type_id, aggContext);
        } else {
            // Add all ranges from multirange
            for (int32 i = 0; i < rangeCount; i++)
                accumArrayResult(state, RangeTypePGetDatum(ranges[i]),
                               false, rangeTypcache->type_id, aggContext);
        }
    }

    PG_RETURN_POINTER(state);
}
```