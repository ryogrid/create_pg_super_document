# get_typlenbyvalalign

## Location
[src/backend/utils/cache/lsyscache.c:2271-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2271-L2302)

## Overview
Retrieves three essential type attributes (length, pass-by-value status, and alignment) for a PostgreSQL data type in a single efficient system cache lookup, providing complete information needed for proper data layout and manipulation.

## Definition
```c
void get_typlenbyvalalign(Oid typid, int16 *typlen, bool *typbyval, char *typalign)
```

## Detailed Description
The `get_typlenbyvalalign` function extends the functionality of `get_typlenbyval` by additionally retrieving the type's alignment requirement (`typalign`). This "three-fer" function provides all the essential type information needed for proper data layout, memory management, and tuple construction in a single system cache lookup.

The alignment information is crucial for ensuring that data is stored at proper memory boundaries, which is required for correct operation on many CPU architectures and for optimal performance. Together with length and pass-by-value status, these three attributes completely specify how to handle a type's values in memory, making this function essential for tuple construction, array operations, and any code that needs to layout data structures containing multiple types.

This function is extensively used throughout PostgreSQL's array handling, type casting, indexing, and data serialization code where complete type layout information is required.

## Parameters / Member Variables
- `typid`: The OID (Object Identifier) of the PostgreSQL data type to look up
- `typlen`: Pointer to int16 where the type's storage length will be stored (-1 for variable-length types)
- `typbyval`: Pointer to bool where the type's pass-by-value status will be stored (true if passed by value, false if by reference)
- `typalign`: Pointer to char where the type's alignment requirement will be stored ('c'=char, 's'=short, 'i'=int, 'd'=double)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - elog (error logging and reporting)
  - Form_pg_type (type catalog structure)
- Called from (representative examples):
  - [ginarrayextract](ginarrayextract.md) (GIN array extraction)
  - [ginqueryarrayextract](ginqueryarrayextract.md) (GIN query array extraction)
  - [_bt_preprocess_array_keys](../b/_bt_preprocess_array_keys.md) (B-tree array key preprocessing)
  - [CreateCast](../C/CreateCast.md) (type cast creation)
  - [DefineRange](../D/DefineRange.md) (range type definition)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [ExecEvalScalarArrayOp](../E/ExecEvalScalarArrayOp.md) (scalar array operation evaluation)
  - [array_position_common](../a/array_position_common.md) (array position functions)
  - [array_map](../a/array_map.md) (array mapping operations)
  - [array_create_iterator](../a/array_create_iterator.md) (array iterator creation)
  - scalararraysel (scalar array selectivity estimation)

## Notes and Other Information
- Raises an ERROR if the type OID is invalid or not found
- The alignment values represent: 'c' (1-byte), 's' (2-byte), 'i' (4-byte), 'd' (8-byte) alignment
- Most efficient way to get all three type attributes when all are needed
- Critical for array operations, tuple construction, and memory layout calculations
- Used extensively in executor nodes dealing with arrays and complex data types
- Essential for ensuring proper data alignment on all supported CPU architectures
- Prefer this function over multiple separate calls when all three values are required