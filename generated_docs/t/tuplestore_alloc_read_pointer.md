# tuplestore_alloc_read_pointer

## Location
src/backend/utils/sort/tuplestore.c: 383 - 417

## Overview
Function to allocate an additional read pointer for a tuplestore, enabling multiple concurrent reading positions within the same tuplestore.

## Definition


## Detailed Description
This function creates a new read pointer that allows independent positioning within the tuplestore data. Multiple read pointers enable scenarios where different parts of the code need to scan through the tuplestore at different positions simultaneously. The new read pointer initially copies the position of read pointer 0, then can be moved independently.

The function enforces capability constraints to ensure that adding new requirements after data insertion doesn't violate the tuplestore's established execution strategy. If data has already been inserted, the new eflags must not increase the overall requirements beyond what was previously established.

The read pointer array is dynamically resized if necessary, doubling in size when the current capacity is exceeded.

## Parameters / Member Variables
- : Pointer to the Tuplestorestate structure to extend with a new read pointer
- : Execution flags for the new read pointer defining its scanning capabilities

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
  - ERROR (error level constant)
  - repalloc (memory reallocation function)
  - TSS_INMEM (tuplestore status constant)
- Data structures used:
  - Tuplestorestate (main tuplestore state structure)
  - TSReadPointer (read pointer structure)
- Called from:
  - ExecInitCteScan (CTE scan node initialization)
  - ExecMaterial (material node execution)
  - ExecInitNamedTuplestoreScan (named tuplestore scan initialization)
  - begin_partition (window aggregation partitioning - multiple calls for different pointers)

## Notes and Other Information
- Returns the index of the newly allocated read pointer
- New pointer initially copies position and state from read pointer 0
- After data insertion, new eflags cannot increase tuplestore requirements
- Read pointer array grows dynamically, doubling in size when needed
- Each read pointer can have independent eflags and position
- Commonly used in window functions that need multiple scanning positions
- The returned index is used in subsequent tuplestore operations to specify which read pointer to use