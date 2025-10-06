# range_constructor3

## Location
[src/backend/utils/adt/rangetypes.c:406-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L406-L445)

## Overview
Creates a general range value from three arguments, allowing explicit specification of boundary inclusiveness through a flags parameter.

## Definition
Datum range_constructor3(PG_FUNCTION_ARGS)

## Detailed Description
The `range_constructor3` function constructs a PostgreSQL range type from three input arguments: lower bound, upper bound, and a flags string that specifies the inclusiveness of each boundary. This provides maximum flexibility for range construction, allowing users to specify whether each bound should be inclusive or exclusive. The function parses the flags argument to determine boundary behavior and validates that the flags argument is not null.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1` (Datum): Lower bound value (treated as negative infinity if NULL)
  - `arg2` (Datum): Upper bound value (treated as positive infinity if NULL)
  - Third argument (text): Flags string specifying boundary inclusiveness (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md) - Gets the return type (range type) from function call info
  - `RangeBound` - Structure representing range boundary information
  - [range_get_typcache](range_get_typcache.md) - Retrieves type cache entry for the range type
  - [range_parse_flags](range_parse_flags.md) - Parses the flags string to determine boundary inclusiveness
  - `[text_to_cstring](../t/text_to_cstring.md)` - Converts PostgreSQL text type to C string
  - `RANGE_LB_INC` - Flag constant for inclusive lower bound
  - `RANGE_UB_INC` - Flag constant for inclusive upper bound
  - [make_range](../m/make_range.md) - Constructs and validates the actual range value
  - `PG_RETURN_RANGE_P` - Returns the constructed range as a PostgreSQL Datum
- Called from (representative examples):
  - SQL range constructor functions with explicit boundary specification
  - Advanced range operations requiring custom boundary behavior

## Notes and Other Information
- The flags parameter must not be null, otherwise an error is raised
- Flags are parsed to determine whether each boundary is inclusive or exclusive
- This is the most flexible range constructor, allowing all four possible boundary combinations: (), (], [), []
- NULL lower/upper bounds are still interpreted as infinite bounds
- The function provides complete control over range boundary semantics

## Simplified Source

```c
Datum range_constructor3(PG_FUNCTION_ARGS) {
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    Oid rngtypid = get_fn_expr_rettype(fcinfo->flinfo);
    TypeCacheEntry *typcache;
    RangeBound lower, upper;
    char flags;

    // Get type cache for this range type
    typcache = range_get_typcache(fcinfo, rngtypid);

    // Validate flags argument is not null
    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                       errmsg("range constructor flags argument must not be null")));

    // Parse boundary flags
    flags = range_parse_flags(text_to_cstring(PG_GETARG_TEXT_PP(2)));

    // Set up lower bound
    lower.val = PG_ARGISNULL(0) ? (Datum) 0 : arg1;
    lower.infinite = PG_ARGISNULL(0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    // Set up upper bound
    upper.val = PG_ARGISNULL(1) ? (Datum) 0 : arg2;
    upper.infinite = PG_ARGISNULL(1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    // Create and return the range
    RangeType *range = make_range(typcache, &lower, &upper, false, NULL);
    PG_RETURN_RANGE_P(range);
}
```