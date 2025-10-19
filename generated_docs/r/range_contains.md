# range_contains

## Location
[src/backend/utils/adt/rangetypes.c:638-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L638-L650)

## Overview
The  function determines whether one range completely contains another range, implementing the PostgreSQL range containment operator (@>).

## Definition

```c
Datum
range_contains(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the range containment check in PostgreSQL's range type system. It takes two range arguments and returns a boolean value indicating whether the first range completely contains the second range. The function serves as the SQL-callable wrapper for the internal  function, handling the PostgreSQL function call protocol and type cache management.

The containment relationship means that every element that belongs to the second range also belongs to the first range. This includes cases where the ranges are identical (a range contains itself).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : The first range (potential container) - accessed via   
  - : The second range (potential containee) - accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts range arguments from function call
  -  - Retrieves type cache information for range operations
  -  - Gets the OID of the range type
  -  - Performs the actual containment logic
  -  - Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator @>)

## Notes and Other Information
- This function is typically invoked through the PostgreSQL SQL operator  for range containment
- The actual containment logic is delegated to  which handles the detailed comparison
- Uses PostgreSQL's type cache system for efficient type-specific operations
- Located in src/backend/utils/adt/rangetypes.c:638-650

## Simplified Source

```c
Datum range_contains(PG_FUNCTION_ARGS) {
    // Extract range arguments: r1 @> r2 (does r1 contain r2?)
    RangeType *r1 = PG_GETARG_RANGE_P(0);
    RangeType *r2 = PG_GETARG_RANGE_P(1);

    // Get type cache for this range type
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    // Delegate to internal containment function and return result
    return PG_RETURN_BOOL(range_contains_internal(typcache, r1, r2));
}
``` 