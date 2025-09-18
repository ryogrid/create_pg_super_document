# func_strict

## Location
src/backend/utils/cache/lsyscache.c: 1761 - 1779

## Overview
Retrieves the proisstrict flag for a given function, indicating whether the function is strict (returns NULL if any argument is NULL).

## Definition
```c
bool func_strict(Oid funcid)
```

## Detailed Description
This function performs a system cache lookup to retrieve the proisstrict attribute of a PostgreSQL function. The proisstrict flag is a boolean value stored in the pg_proc system catalog that indicates whether a function is strict. A strict function automatically returns NULL if any of its arguments is NULL, without actually calling the function's implementation. This property is crucial for query optimization, particularly for NULL handling optimizations and predicate analysis.

The function uses PostgreSQL's system cache mechanism for efficient lookups of function metadata. If the function ID is not found, an ERROR is raised.

## Parameters / Member Variables
- `funcid`: Object identifier (Oid) of the function whose proisstrict flag is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - ReleaseSysCache
  - ObjectIdGetDatum
- Called from (representative examples):
  - AggregateCreate (src/backend/catalog/pg_aggregate.c:393, 436, 551)
  - check_and_push_window_quals (src/backend/optimizer/path/allpaths.c:2431)
  - process_equivalence (src/backend/optimizer/path/equivclass.c:186)
  - hash_ok_operator (src/backend/optimizer/plan/subselect.c:857)
  - contain_nonstrict_functions_checker (src/backend/optimizer/util/clauses.c:1001)

## Notes and Other Information
- Part of the lsyscache.c module which provides convenient access functions for system catalog information
- Critical for query optimization, especially in NULL-handling scenarios
- Strict functions enable various optimizations such as predicate pushdown and equivalence class processing
- Used extensively throughout the optimizer for determining when functions can be safely optimized
- The strictness property allows the optimizer to avoid function calls when NULL arguments are detected
- Located in src/backend/utils/cache/lsyscache.c:1761-1779