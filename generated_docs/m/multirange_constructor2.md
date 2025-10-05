# multirange_constructor2

## Location
[src/backend/utils/adt/multirangetypes.c:941-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L941-L1022)

## Overview
Constructs a multirange value from an array of ranges, providing the main entry point for creating multiranges from multiple range inputs.

## Definition

```c
Datum
multirange_constructor2(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL function interface for constructing multirange values from arrays of ranges. It serves as the backend implementation for SQL multirange constructor functions. The function handles various input scenarios including empty arrays, single-dimensional range arrays, and validates that all input ranges are of the correct type. It performs comprehensive error checking for null values, multidimensional arrays, and type mismatches before delegating the actual multirange construction to the  function.

## Parameters / Member Variables
- Function uses PostgreSQL's PG_FUNCTION_ARGS macro which provides access to:

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md) (determines return type)
  - [multirange_get_typcache](multirange_get_typcache.md) (gets type cache information)
  - [make_multirange](make_multirange.md) (constructs the actual multirange)
  - PG_RETURN_MULTIRANGE_P (returns multirange result)
  - ARR_NDIM, ARR_ELEMTYPE (array introspection)
  - [deconstruct_array](../d/deconstruct_array.md) (extracts array elements)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md) (converts Datum to RangeType)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This is a PostgreSQL function callable from SQL (supports VARIADIC arrays)
- Handles edge cases like zero-argument calls and empty arrays
- Validates input array dimensionality (rejects multidimensional arrays)
- Enforces non-null constraints on multirange members
- Performs type checking to ensure array elements match expected range type
- Uses PostgreSQL's memory allocation functions (palloc0)
- Located in src/backend/utils/adt/multirangetypes.c

## Simplified Source

```c
Datum
multirange_constructor2(PG_FUNCTION_ARGS)
{
    Oid multirange_type_id = get_fn_expr_rettype(fcinfo->flinfo);
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, multirange_type_id);
    TypeCacheEntry *range_type = typcache->rngtype;

    // Handle no arguments - return empty multirange
    if (PG_NARGS() == 0)
        PG_RETURN_MULTIRANGE_P(make_multirange(multirange_type_id, range_type, 0, NULL));

    // Validate input is not null
    if (PG_ARGISNULL(0))
        elog(ERROR, "multirange values cannot contain null members");

    ArrayType *range_array = PG_GETARG_ARRAYTYPE_P(0);

    // Validate array is one-dimensional
    int dimensions = ARR_NDIM(range_array);
    if (dimensions > 1)
        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                       errmsg("multiranges cannot be constructed from multidimensional arrays")));

    // Validate element type matches expected range type
    Oid element_type_id = ARR_ELEMTYPE(range_array);
    if (element_type_id != range_type->type_id)
        elog(ERROR, "type %u does not match constructor type", element_type_id);

    // Handle empty array
    if (dimensions == 0)
        PG_RETURN_MULTIRANGE_P(make_multirange(multirange_type_id, range_type, 0, NULL));

    // Extract array elements
    Datum *elements;
    bool *nulls;
    int range_count;
    deconstruct_array(range_array, element_type_id, range_type->typlen,
                     range_type->typbyval, range_type->typalign,
                     &elements, &nulls, &range_count);

    // Convert elements to ranges, checking for nulls
    RangeType **ranges = palloc0(range_count * sizeof(RangeType *));
    for (int i = 0; i < range_count; i++) {
        if (nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("multirange values cannot contain null members")));
        ranges[i] = DatumGetRangeTypeP(elements[i]);
    }

    // Create and return the multirange
    PG_RETURN_MULTIRANGE_P(make_multirange(multirange_type_id, range_type, range_count, ranges));
}
```

This function constructs a multirange from an array of ranges, performing validation and error checking before delegating to `make_multirange`.