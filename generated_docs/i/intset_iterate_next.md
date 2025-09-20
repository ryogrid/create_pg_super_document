# intset_iterate_next

## Location
[src/backend/lib/integerset.c:643-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L643-L713)

## Overview
Returns the next integer when iterating through an IntegerSet, providing sequential access to all values stored in the set.

## Definition

```c
bool
intset_iterate_next(IntegerSet *intset, uint64 *next)
```
## Detailed Description
This function is part of PostgreSQL's IntegerSet iteration mechanism. It retrieves the next value during sequential iteration through the set, which must be initiated with . The function handles the complex internal structure of IntegerSet, which stores integers in both B-tree leaf nodes (compressed using Simple8b encoding) and a buffer for newly-added values.

The function operates in several phases:
1. First returns values from the current decoded buffer ()
2. When the buffer is exhausted, decodes the next item from the current leaf node using Simple8b decompression
3. When a leaf node is exhausted, moves to the next leaf node in the B-tree
4. When the B-tree is exhausted, processes any buffered values not yet committed to the B-tree
5. Finally returns false when no more values exist

## Parameters / Member Variables
- : Pointer to the IntegerSet being iterated over
- : Pointer to uint64 where the next value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - : Decompresses Simple8b encoded values from leaf items
- Called from (representative examples):
  - : Used in GiST index vacuum operations
  - : Used in integerset test suite
  - : Used in integerset test suite
  - : Used in integerset test suite

## Notes and Other Information
- Must be called after  to initialize iteration state
- Returns true if a value was retrieved, false when iteration is complete
- The function automatically handles the transition between different storage phases (decoded buffer, B-tree nodes, unbuffered values)
- Uses Simple8b compression for efficient storage of integer sequences in B-tree leaf nodes
- The iteration state is tracked through multiple fields in the IntegerSet structure (, , , etc.)