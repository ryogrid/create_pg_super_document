# oidvectoreq

## Location
[src/backend/utils/adt/oid.c:344-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L344-L351)

## Overview
The  function implements the equality comparison operator for PostgreSQL's oidvector data type, determining if two oidvector values are equal.

## Definition

```c
Datum
oidvectoreq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the SQL operator  for oidvector values in PostgreSQL. An oidvector is an array-like data type that stores multiple OID values, commonly used in system catalogs to represent lists of object identifiers. The function delegates the actual comparison to , which performs a comprehensive comparison of the two oidvector structures, and then checks if the result indicates equality (comparison result equals 0). This follows the standard pattern where comparison functions return 0 for equality, negative for less-than, and positive for greater-than.

## Parameters / Member Variables
- : Function call information structure containing the two oidvector operands to compare
- : The comparison result from  (0 = equal, <0 = first < second, >0 = first > second)

## Dependencies
- Functions called/Symbols referenced:
  -  (B-tree comparison function for oidvector types)
  -  (macro for extracting int32 values from Datum)
  -  (macro for returning boolean results)
- Called from (representative examples):
  -  (in src/backend/utils/cache/catcache.c:263)
  - SQL queries using  operator on oidvector columns
  - PostgreSQL's operator dispatch system

## Notes and Other Information
- This function is part of PostgreSQL's built-in operator set for the oidvector data type
- The function is located in 
- Oidvectors are primarily used in system catalogs like pg_proc.proargtypes to store arrays of type OIDs
- The equality check leverages the existing B-tree comparison infrastructure for consistency
- The function name follows PostgreSQL's naming convention for comparison operators (oidvector + eq for 'equal')