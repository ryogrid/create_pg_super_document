# HeapKeyTest

## Location
[src/include/access/valid.h:28-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/valid.h#L28-L58)

## Overview
HeapKeyTest is an inline function that tests whether a heap tuple satisfies a scan key condition, returning true if all specified scan key conditions are met and false otherwise.

## Definition

```c
static inline bool
HeapKeyTest(HeapTuple tuple, TupleDesc tupdesc, int nkeys, ScanKey keys)
```
## Detailed Description
HeapKeyTest performs a sequential evaluation of scan key conditions against a heap tuple. It iterates through all provided scan keys and tests each condition against the corresponding attribute value in the tuple. The function implements early termination - if any single condition fails, it immediately returns false without evaluating remaining conditions.

The function performs the following key operations:
1. Iterates through each scan key in the provided array
2. Checks for NULL scan key flags (SK_ISNULL) and fails immediately if found
3. Extracts the attribute value from the tuple using heap_getattr
4. Returns false if the attribute value is NULL
5. Calls the comparison function specified in the scan key with proper collation
6. Returns false if any comparison function returns false
7. Returns true only if all scan key conditions are satisfied

This function is a critical component of PostgreSQL's tuple scanning mechanism, providing efficient filtering during table scans and index operations.

## Parameters / Member Variables
- : HeapTuple pointer to the heap tuple being tested
- : TupleDesc pointer describing the tuple's structure and attributes  
- : Integer specifying the number of scan keys to evaluate
- : ScanKey array containing the scan key conditions to test against

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md): Extracts attribute values from heap tuples
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md): Executes comparison functions with collation support
  - [DatumGetBool](../D/DatumGetBool.md): Converts Datum result to boolean value
  - SK_ISNULL: Flag constant indicating NULL scan key condition
- Called from (representative examples):
  - [heapgettup](../h/heapgettup.md): Sequential heap tuple scanning in heapam.c:951
  - [heapgettup_pagemode](../h/heapgettup_pagemode.md): Page-mode heap tuple scanning in heapam.c:1055

## Notes and Other Information
- This function is defined as static inline in src/include/access/valid.h for optimal performance
- Early termination optimization ensures minimal overhead when scan conditions fail
- The function handles both user-defined and system attributes through heap_getattr
- NULL handling is strict - both NULL scan keys and NULL attribute values cause immediate failure
- Collation support is provided through FunctionCall2Coll for proper string comparisons
- This function is fundamental to PostgreSQL's query execution engine and is called frequently during table scans