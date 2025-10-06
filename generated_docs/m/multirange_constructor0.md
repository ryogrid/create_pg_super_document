# multirange_constructor0

## Location
[src/backend/utils/adt/multirangetypes.c:1059-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1059-L1081)

## Overview
Constructs an empty multirange value with no range elements, serving as a zero-argument constructor for multirange types.

## Definition
```c
Datum multirange_constructor0(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates an empty multirange containing no range elements. It exists as a separate function from multirange_constructor1 due to PostgreSQL's opr_sanity requirements, which mandate that the same internal function cannot handle multiple functions with different argument counts. The function validates that no arguments are provided and then constructs an empty multirange using the make_multirange function with a count of 0 and NULL range array.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure (should contain no arguments for this niladic constructor)

## Dependencies
- Functions called/Symbols referenced:
  - PG_NARGS
  - [get_fn_expr_rettype](../g/get_fn_expr_rettype.md)
  - [multirange_get_typcache](multirange_get_typcache.md)
  - [make_multirange](make_multirange.md)
  - PG_RETURN_MULTIRANGE_P
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- Specifically designed as a niladic (zero-argument) constructor
- Enforces that no arguments are passed, raising an error if any are provided
- Creates an empty multirange by calling make_multirange with count=0 and ranges=NULL
- Exists separately from other constructors due to PostgreSQL's internal function handling requirements
- Located in src/backend/utils/adt/multirangetypes.c:1059-1081

## Simplified Source

```c
Datum
multirange_constructor0(PG_FUNCTION_ARGS)
{
    // Validate no arguments are passed
    if (PG_NARGS() != 0)
        elog(ERROR, "niladic multirange constructor must not receive arguments");

    // Get type information
    Oid multirange_type_id = get_fn_expr_rettype(fcinfo->flinfo);
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, multirange_type_id);
    TypeCacheEntry *range_type = typcache->rngtype;

    // Create empty multirange
    PG_RETURN_MULTIRANGE_P(make_multirange(multirange_type_id, range_type, 0, NULL));
}
```

This zero-argument constructor creates an empty multirange by calling `make_multirange` with no ranges (count = 0, ranges = NULL).