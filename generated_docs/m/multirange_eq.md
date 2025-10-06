# multirange_eq

## Location
[src/backend/utils/adt/multirangetypes.c:1901-1913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1901-L1913)

## Overview
Implements the equality operator (=) for multirange types, comparing two multirange values to determine if they contain exactly the same set of ranges in the same order.

## Definition

```c
Datum
multirange_eq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the SQL-callable wrapper for multirange equality comparison. It extracts two multirange arguments from the function call context, retrieves the appropriate type cache entry for the multirange type, and delegates the actual comparison logic to the internal  function. The function follows PostgreSQL's standard function calling convention using the  macro and returns a boolean result wrapped in a .

The equality comparison requires that both multiranges have the same number of ranges and that each corresponding range pair has identical lower and upper bounds. The comparison is performed element-wise in order, ensuring both structural and content equality.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : First multirange value ()
  - : Second multirange value ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts multirange arguments from function context
  - : Gets the OID of the multirange type
  - : Retrieves type cache entry for the multirange type
  - : Performs the actual equality comparison logic
  - : Returns boolean result as a Datum
- Called from (representative examples):
  - SQL equality operations on multirange types
  - Internal range comparison operations

## Notes and Other Information
- This is the external interface for multirange equality comparison, callable from SQL
- The function assumes both arguments are of the same multirange type (enforced by PostgreSQL's type system)
- Type cache lookup is performed to access range-specific comparison functions
- The actual comparison logic handles empty multiranges, overlapping ranges, and ensures canonical ordering

## Simplified Source

```c
Datum multirange_eq(PG_FUNCTION_ARGS)
{
    // Extract multirange arguments from function call
    MultirangeType *mr1 = PG_GETARG_MULTIRANGE_P(0);
    MultirangeType *mr2 = PG_GETARG_MULTIRANGE_P(1);

    // Get type cache for range operations
    TypeCacheEntry *typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    // Delegate to internal equality function and return result
    PG_RETURN_BOOL(multirange_eq_internal(typcache->rngtype, mr1, mr2));
}
``` 