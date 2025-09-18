# heap_expand_tuple

## Location
[src/backend/access/common/heaptuple.c:1065-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1065-L1079)

## Overview
Public wrapper function that creates an expanded HeapTuple from a source tuple with fewer attributes, using default values for missing attributes.

## Definition


## Detailed Description
The  function is a public wrapper around the internal  function specifically for creating HeapTuple outputs. This function is used when you need to expand a tuple to match a larger tuple descriptor and want the result as a full HeapTuple rather than a MinimalTuple.

This function is essential for schema evolution scenarios where existing tuples need to be adapted to evolved table schemas that have additional columns. The function ensures that the resulting HeapTuple has the proper structure and metadata expected by the rest of the PostgreSQL system, including proper tuple headers, OIDs, and other HeapTuple-specific information.

## Parameters / Member Variables
- : The source HeapTuple that has fewer attributes than required
- : The target tuple descriptor that defines the required number and types of attributes

## Dependencies
- Functions called/Symbols referenced:
  - [expand_tuple](../e/expand_tuple.md) (internal expansion function)
  - HeapTuple (return type)
- Called from (representative examples):
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Returns a newly allocated HeapTuple with expanded attributes
- Wrapper function that simplifies the interface to expand_tuple for HeapTuple creation
- Used when full HeapTuple format is required (as opposed to MinimalTuple)
- Source tuple must have fewer attributes than the target tuple descriptor
- Missing attributes are filled with default values from tuple descriptor constraints or NULLs
- Essential for handling schema evolution in PostgreSQL
- Part of the public tuple manipulation API
- Located in src/backend/access/common/heaptuple.c:1065-1079