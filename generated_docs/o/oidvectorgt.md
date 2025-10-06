# oidvectorgt

## Location
[src/backend/utils/adt/oid.c:384-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L384-L389)

## Overview
PostgreSQL comparison function that determines if the first oidvector is greater than the second oidvector, implementing the ">" operator for oidvector data types.

## Definition

```c
Datum
oidvectorgt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "greater than" operator (>) for oidvector data types in PostgreSQL. It utilizes the  comparison function to perform a comprehensive comparison between two oidvector values and returns true if the first vector is lexicographically greater than the second vector.

The comparison follows the standard hierarchical approach consistent with other oidvector comparison functions: vectors are first compared by their dimensions (length), and if dimensions are equal, they are compared element-by-element from left to right. The first vector is considered "greater than" the second if it has more dimensions, or if it has the same number of dimensions but contains a larger OID value at the first differing position.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: First oidvector (left operand)
  - Argument 1: Second oidvector (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - : Core comparison function that returns integer comparison result (-1, 0, or 1)
  - : Macro to extract int32 value from Datum
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's operator implementation system for oidvector types
- Returns boolean true when the comparison result is positive (first > second), false otherwise
- The underlying  function returns positive values when the first argument is greater than the second
- Typically invoked through SQL expressions using the > operator on oidvector columns
- Essential for ORDER BY clauses and comparison operations involving oidvector data
- Located in src/backend/utils/adt/oid.c:384-389

## Simplified Source

```c
Datum
oidvectorgt(PG_FUNCTION_ARGS)
{
    // Compare two oidvectors using btree comparison function
    int32 cmp = DatumGetInt32(btoidvectorcmp(fcinfo));

    // Return true if first > second (comparison result > 0)
    PG_RETURN_BOOL(cmp > 0);
}
```