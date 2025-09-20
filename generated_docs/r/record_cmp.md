# record_cmp

## Location
[src/backend/utils/adt/rowtypes.c:823-1066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L823-L1066)

## Overview
Internal comparison function for composite type (record) values that implements element-by-element comparison logic for PostgreSQL's record comparison operations.

## Definition

```c
structures */
	tuple1.t_len = HeapTupleHeaderGetDatumLength(record1);
```
## Detailed Description
The  function is the core comparison engine for composite types in PostgreSQL. It performs a lexicographic comparison between two record values by comparing corresponding columns in order. The function handles different record types as long as they have the same number of non-dropped columns with compatible types. It implements PostgreSQL's NULL comparison semantics where NULL values are considered greater than any non-NULL value, and two NULL values are considered equal.

The function extracts type information from both tuple headers, validates column type compatibility, and performs element-by-element comparison using the appropriate type-specific comparison functions. It handles dropped columns by skipping them and maintains comparison metadata cache for performance optimization across repeated calls.

## Parameters / Member Variables
- : Function call information containing two  arguments representing the records to compare
- Returns: Integer (-1, 0, 1) indicating first record is less than, equal to, or greater than second record

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple headers
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple headers  
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptors for both record types
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Extracts individual column values from both tuples
  - [lookup_type_cache](../l/lookup_type_cache.md): Gets type cache entries with comparison function info
  - FunctionCallInvoke: Invokes type-specific comparison functions
  - [format_type_be](../f/format_type_be.md): Formats type names for error messages
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor references

- Called from (representative examples):
  - [record_lt](record_lt.md): Less than comparison operator for records (src/backend/utils/adt/rowtypes.c:1291)
  - [record_gt](record_gt.md): Greater than comparison operator for records (src/backend/utils/adt/rowtypes.c:1297)
  - [record_le](record_le.md): Less than or equal comparison operator for records (src/backend/utils/adt/rowtypes.c:1303)
  - [record_ge](record_ge.md): Greater than or equal comparison operator for records (src/backend/utils/adt/rowtypes.c:1309)
  - [btrecordcmp](../b/btrecordcmp.md): B-tree comparison function for records (src/backend/utils/adt/rowtypes.c:1315)

## Notes and Other Information
- Implements lexicographic comparison: compares columns left-to-right until finding unequal values
- Handles heterogeneous record types (e.g., anonymous ROW() vs named composite types) 
- Enforces strict type compatibility: corresponding columns must have identical type OIDs
- Uses PostgreSQL's NULL comparison semantics: NULL > any non-NULL value, NULL == NULL
- Handles collation differences gracefully by passing InvalidOid when collations don't match
- Uses function-local caching (fn_extra) to optimize repeated calls with same record types
- Validates column count consistency between record types
- Memory management includes protection against toasted input values
- Core foundation for all record comparison operators and B-tree indexing support