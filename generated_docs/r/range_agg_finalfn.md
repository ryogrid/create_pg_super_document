# range_agg_finalfn

## Location
[src/backend/utils/adt/multirangetypes.c:1372-1411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1372-L1411)

## Overview
The finalize function for the range_agg aggregate that converts the collected range values into a multirange result.

## Definition
Datum range_agg_finalfn(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the finalize function for the range_agg aggregate operation in PostgreSQL. It takes the array of range values accumulated by range_agg_transfn and converts them into a multirange result. The function extracts the collected ranges from the ArrayBuildState and constructs a new multirange containing all the input ranges.

Unlike what the comment suggests about merging touching ranges, this function actually delegates the merging logic to make_multirange, which handles the sorting and combining of overlapping/adjacent ranges. The function handles edge cases such as empty aggregation results and validates the aggregate context.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call interface containing:
  - Argument 0: ArrayBuildState pointer containing accumulated range values

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md)
  - [multirange_get_typcache](../m/multirange_get_typcache.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - [make_multirange](../m/make_multirange.md)
  - PG_RETURN_MULTIRANGE_P
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_RETURN_NULL
  - [palloc0](../p/palloc0.md)
  - elog
- Called from:
  - No direct callers found (called through SQL aggregate function interface)

## Notes and Other Information
- Part of PostgreSQL's range aggregation system, working with range_agg_transfn
- Returns NULL if no input ranges were provided (standard aggregate behavior)
- Validates aggregate context before processing
- Converts accumulated ranges into a multirange result
- The actual range merging/combining is handled by make_multirange
- Shared implementation logic with multirange_agg_finalfn
- Returns a multirange type as the final aggregate result
- Located in src/backend/utils/adt/multirangetypes.c:1372-1411

## Simplified Source

```c
Datum range_agg_finalfn(PG_FUNCTION_ARGS) {
    MemoryContext aggContext;
    Oid multirangeTypeOid;
    TypeCacheEntry *typcache;
    ArrayBuildState *state;
    int32 rangeCount;
    RangeType **ranges;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, &aggContext))
        elog(ERROR, "range_agg_finalfn called in non-aggregate context");

    // Get accumulated state
    state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
    if (state == NULL || state->nelems == 0)
        PG_RETURN_NULL();

    // Get return type and cache info
    multirangeTypeOid = get_fn_expr_rettype(fcinfo->flinfo);
    typcache = multirange_get_typcache(fcinfo, multirangeTypeOid);

    // Extract ranges from state
    rangeCount = state->nelems;
    ranges = palloc0(rangeCount * sizeof(RangeType *));
    for (int i = 0; i < rangeCount; i++)
        ranges[i] = DatumGetRangeTypeP(state->dvalues[i]);

    // Create and return multirange
    PG_RETURN_MULTIRANGE_P(make_multirange(multirangeTypeOid, typcache->rngtype,
                                          rangeCount, ranges));
}
```