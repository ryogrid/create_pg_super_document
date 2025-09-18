# record_eq

## Location
[src/backend/utils/adt/rowtypes.c:1067-1282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1067-L1282)

## Overview
Compares two records (row types) for equality, returning true if all corresponding column values are equal.

## Definition


## Detailed Description
The  function performs field-by-field comparison of two PostgreSQL records to determine equality. It handles records with potentially different structures by:

1. Extracting type information from both record headers
2. Building temporary HeapTuple control structures
3. Caching comparison metadata to optimize repeated calls
4. Deforming tuples into individual column values and null flags
5. Comparing corresponding columns while handling:
   - Dropped columns (skipped during comparison)
   - Type mismatches (raises error)
   - NULL values (two NULLs are considered equal, NULL ≠ non-NULL)
   - Column count mismatches (raises error if structures differ)

The function uses the type cache system to look up appropriate equality operators for each column type and employs stack depth checking to prevent infinite recursion when dealing with nested record types.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Prevents stack overflow in recursive comparisons
  - : Extracts record type OID
  - : Extracts type modifier
  - : Gets tuple descriptor for record type
  - : Breaks record into individual column values
  - : Caches equality operator information
  - : Allocates comparison metadata cache
  - : Calls column-specific equality functions
- Called from (representative examples):
  - : Uses record_eq and negates the result

## Notes and Other Information
- Does not use  for comparison since equality can be meaningful for types without total ordering
- Caches comparison metadata () in  to optimize repeated calls with same record types
- Handles structural differences gracefully by skipping dropped columns
- Raises errors for type mismatches and column count differences
- Memory management includes cleanup of temporary allocations and toasted input handling
- Supports collation-aware comparisons when column collations match