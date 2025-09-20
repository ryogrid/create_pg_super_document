# minimal_expand_tuple

## Location
[src/backend/access/common/heaptuple.c:1053-1064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1053-L1064)

## Overview
Public wrapper function that creates an expanded MinimalTuple from a source tuple with fewer attributes, using default values for missing attributes.

## Definition

```c
MinimalTuple
minimal_expand_tuple(HeapTuple sourceTuple, TupleDesc tupleDesc)
```
## Detailed Description
The  function is a simple public wrapper around the internal  function specifically for creating MinimalTuple outputs. This function is used when you need to expand a tuple to match a larger tuple descriptor but want the result as a MinimalTuple rather than a full HeapTuple.

MinimalTuples are more compact representations used primarily for in-memory tuple storage in contexts like sorting, hashing, and temporary tuple storage where the full HeapTuple overhead is unnecessary. This function is essential for operations that need to adapt existing tuples to evolved schemas while maintaining the MinimalTuple format.

## Parameters / Member Variables
- : The source HeapTuple that has fewer attributes than required
- : The target tuple descriptor that defines the required number and types of attributes

## Dependencies
- Functions called/Symbols referenced:
  - [expand_tuple](../e/expand_tuple.md) (internal expansion function)
  - MinimalTuple (return type)
- Called from (representative examples):
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Returns a newly allocated MinimalTuple with expanded attributes
- Wrapper function that simplifies the interface to expand_tuple for MinimalTuple creation
- Used when MinimalTuple format is preferred for memory efficiency
- Source tuple must have fewer attributes than the target tuple descriptor
- Missing attributes are filled with default values from tuple descriptor constraints or NULLs
- Part of the public tuple manipulation API
- Located in src/backend/access/common/heaptuple.c:1053-1064