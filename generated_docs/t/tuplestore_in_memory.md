# tuplestore_in_memory

## Location
src/backend/utils/sort/tuplestore.c: 1455 - 1465

## Overview
A simple utility function that checks whether a tuplestore is currently operating in memory-only mode and has not spilled to disk.

## Definition
```c
bool tuplestore_in_memory(Tuplestorestate *state)
```

## Detailed Description
This function provides a way to query whether a tuplestore is still operating entirely in memory or has transitioned to using temporary disk files. It simply checks the internal status flag of the tuplestore state. The function is noted in comments as potentially violating modularity principles, suggesting it may be refactored in future versions to better encapsulate the tuplestore implementation details.

## Parameters / Member Variables
- `state`: The tuplestore state to check for memory-only operation

## Dependencies
- Functions called/Symbols referenced:
  - Tuplestorestate
  - TSS_INMEM (tuplestore status constant indicating in-memory mode)
- Called from (representative examples):
  - [spool_tuples](../s/spool_tuples.md)

## Notes and Other Information
- Returns true if the tuplestore status is TSS_INMEM, false otherwise
- The comment indicates this function may violate modularity principles by exposing internal implementation details
- Primarily used by callers who need to make performance or behavior decisions based on whether data has spilled to disk
- Simple boolean check with no side effects or complex logic