# construct_empty_expanded_array

## Location
[src/backend/utils/adt/arrayfuncs.c:3585-3618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3585-L3618)

## Overview
Creates an empty expanded array object that provides an optimized in-memory representation for efficient array operations and modifications.

## Definition

```c
ExpandedArrayHeader *
construct_empty_expanded_array(Oid element_type,
							   MemoryContext parentcontext,
							   ArrayMetaState *metacache)
```
## Detailed Description
The construct_empty_expanded_array function creates an empty array in PostgreSQL's expanded object format, which provides an optimized in-memory representation designed for efficient array operations. Unlike regular ArrayType objects, expanded arrays maintain additional metadata and use structures that facilitate faster element access, modification, and array operations.

The function first creates a regular empty array using construct_empty_array, then converts it to the expanded format using expand_array. The original ArrayType structure is freed after conversion since the expanded form contains all necessary data. The expanded array can optionally use cached metadata for improved performance in repeated operations.

## Parameters / Member Variables
- `element_type`: OID of the data type that the empty expanded array would contain if it had elements
- `parentcontext`: Memory context in which the expanded array should be allocated
- `*metacache`: Optional ArrayMetaState structure containing cached type metadata (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [construct_empty_array](construct_empty_array.md)
  - [expand_array](../e/expand_array.md)
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - ExpandedArrayHeader (type)
  - [ArrayMetaState](../A/ArrayMetaState.md) (type)
- Called from (representative examples):
  - [fetch_array_arg_replace_nulls](../f/fetch_array_arg_replace_nulls.md)

## Notes and Other Information
- Expanded arrays provide better performance for operations that involve frequent element access or modifications
- The metacache parameter allows reuse of type information across multiple array operations for better performance
- The function manages memory carefully by freeing the temporary ArrayType after expansion
- Expanded arrays are part of PostgreSQL's expanded object infrastructure for optimizing composite data types
- The returned ExpandedArrayHeader provides access to both the array data and additional metadata for efficient operations
- This function is less commonly used than other array constructors due to the specialized nature of expanded arrays