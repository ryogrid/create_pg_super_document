# comparison_shim

## Location
[src/backend/utils/sort/sortsupport.c:43-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sortsupport.c#L43-L67)

## Overview
A static shim function that adapts old-style PostgreSQL comparison functions to work with the newer SortSupport framework by wrapping function calls.

## Definition

```c
static int
comparison_shim(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
The comparison_shim function serves as an adapter between PostgreSQL's modern SortSupport framework and legacy comparison functions that don't natively support the SortSupport interface. It acts as a lightweight wrapper that:

1. Extracts the SortShimExtra structure from the SortSupport's extra data
2. Sets up function call arguments with the two Datum values to compare
3. Invokes the underlying comparison function using FunctionCallInvoke
4. Validates the result and returns the comparison result

This shim is essentially an inlined, optimized version of FunctionCall2Coll(), with the assumption that most of the FunctionCallInfoBaseData structure was already initialized by PrepareSortSupportComparisonShim.

## Parameters / Member Variables
- `x`: First Datum value to compare
- `y`: Second Datum value to compare
- `ssup`: SortSupport structure containing the extra data with function call information
## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (type)
  - [SortShimExtra](../S/SortShimExtra.md) (type)
  - FunctionCallInvoke
- Called from:
  - [PrepareSortSupportComparisonShim](../P/PrepareSortSupportComparisonShim.md) (at src/backend/utils/sort/sortsupport.c:85)

## Notes and Other Information
- This is a static function, only accessible within sortsupport.c
- The function includes paranoia checks to reset isnull flag and validate non-null results
- Designed for performance - avoids repeated setup overhead by reusing pre-configured function call structures
- Part of PostgreSQL's sort support optimization framework that allows data types to provide specialized, efficient comparison functions

## Simplified Source

```c
static int
comparison_shim(Datum x, Datum y, SortSupport ssup)
{
    SortShimExtra *extra = (SortShimExtra *) ssup->ssup_extra;
    Datum result;

    // Set up function arguments
    extra->fcinfo.args[0].value = x;
    extra->fcinfo.args[1].value = y;

    // Reset null flag for safety
    extra->fcinfo.isnull = false;

    // Call the comparison function
    result = FunctionCallInvoke(&extra->fcinfo);

    // Ensure result is not NULL
    if (extra->fcinfo.isnull)
        elog(ERROR, "function %u returned NULL", extra->flinfo.fn_oid);

    return result;
}
```