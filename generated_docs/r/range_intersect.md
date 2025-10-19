# range_intersect

## Location
[src/backend/utils/adt/rangetypes.c:1127-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1127-L1142)

## Overview
Computes the intersection of two range values, returning a new range that contains only the overlap between the input ranges.

## Definition

```c
Datum
range_intersect(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that performs set intersection on two range values. It serves as a wrapper around the internal  function, handling argument validation and type checking before delegating the actual intersection logic. The function ensures that both input ranges are of the same range type before proceeding with the intersection operation.

The function follows PostgreSQL's standard function calling conventions using the  macro and returns a  value that represents the resulting range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): First range value ()  
  - Second argument (index 1): Second range value ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts range arguments from function call
  - : Gets the OID of a range type for type matching
  - : Retrieves type cache information for the range type
  - : Performs the actual intersection computation
  - : Returns the result range value
- Called from:
  - SQL queries using the  operator on range types
  - [Range](../R/Range.md) intersection operations in user applications

## Notes and Other Information
- The function performs strict type checking to ensure both ranges are of the same type, throwing an ERROR if types don't match
- This type checking is primarily a safety measure since PostgreSQL's ANYRANGE matching rules should prevent type mismatches at the SQL level
- The actual intersection logic is delegated to  for code reusability
- Located in src/backend/utils/adt/rangetypes.c

## Simplified Source

```c
Datum range_intersect(PG_FUNCTION_ARGS) {
    // Extract the two range arguments
    RangeType *r1 = PG_GETARG_RANGE_P(0);
    RangeType *r2 = PG_GETARG_RANGE_P(1);

    // Ensure both ranges are of the same type
    if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
        elog(ERROR, "range types do not match");

    // Get type cache for range operations
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    // Compute and return the intersection
    PG_RETURN_RANGE_P(range_intersect_internal(typcache, r1, r2));
}
```