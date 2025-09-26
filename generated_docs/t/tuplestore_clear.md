# tuplestore_clear

## Location
src/backend/utils/sort/tuplestore.c: 418 - 452

## Overview
Function to delete all contents from a tuplestore and reset all read pointers to their initial state, effectively returning the tuplestore to its empty condition.

## Definition

```c
void
tuplestore_clear(Tuplestorestate *state)
```
## Detailed Description
This function performs a complete cleanup of a tuplestore's contents while preserving the tuplestore structure itself for reuse. It releases all stored tuples from memory, closes any temporary files used for disk storage, and resets all internal counters and read pointers to their initial state.

The function handles both memory-resident tuples and disk-based storage, ensuring proper cleanup of resources in either case. Memory accounting is properly maintained through FREEMEM calls, and the tuplestore is returned to TSS_INMEM status regardless of its previous state.

After clearing, the tuplestore is ready to accept new tuples as if it were newly created, but without the overhead of deallocating and reallocating the main data structure.

## Parameters / Member Variables
- : Pointer to the Tuplestorestate structure to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - BufFileClose (closes temporary files)
  - GetMemoryChunkSpace (memory size calculation)
  - FREEMEM (memory accounting macro)
  - pfree (memory deallocation)
  - TSS_INMEM (tuplestore status constant)
- Data structures used:
  - Tuplestorestate (main tuplestore state structure)
  - TSReadPointer (read pointer structure)
- Called from:
  - fmgr_sql (SQL function manager - multiple calls for function execution)
  - ExecReScanCteScan (CTE scan node rescanning)
  - ExecReScanRecursiveUnion (recursive union rescan operations)

## Notes and Other Information
- Preserves the tuplestore structure and configuration for reuse
- Properly handles memory accounting by calling FREEMEM before pfree
- Resets all read pointers to position 0 with eof_reached=false
- Closes temporary files if they exist, freeing associated resources
- Returns tuplestore to TSS_INMEM status regardless of previous state
- Used in rescan operations where the same tuplestore needs to be reused with new data
- More efficient than destroying and recreating a tuplestore when the same configuration is needed
- Does not affect the tuplestore's capacity settings or execution flags