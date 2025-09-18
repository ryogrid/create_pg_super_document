# spginsert

## Location
src/backend/access/spgist/spginsert.c: 183 - 219

## Overview
Inserts a single new tuple into an existing SP-GiST index, handling potential conflicts and memory management.

## Definition


## Detailed Description
This function handles the insertion of individual tuples into an SP-GiST index during normal database operations (as opposed to bulk building). It creates a temporary memory context for the insertion process and implements retry logic to handle concurrent insertion conflicts. The function repeatedly calls spgdoinsert() until the insertion succeeds, resetting the memory context and reinitializing the SP-GiST state on each retry to handle conflicts with concurrent operations. After successful insertion, it updates the index metapage and cleans up the temporary context.

## Parameters / Member Variables
- : The SP-GiST index relation to insert into
- : Array of column values for the new tuple
- : Array of boolean flags indicating NULL values
- : Heap tuple ID (item pointer) of the new tuple
- : The heap relation containing the tuple
- : Unique constraint checking mode (unused in SP-GiST)
- : Whether the indexed values are unchanged (for HOT updates)
- : Index metadata and configuration

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [initSpGistState](../i/initSpGistState.md)
  - [spgdoinsert](spgdoinsert.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [SpGistUpdateMetaPage](../S/SpGistUpdateMetaPage.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - SpGistState
- Called from (representative examples):
  - [spghandler](spghandler.md)

## Notes and Other Information
Always returns false since SP-GiST does not support unique constraints. The retry mechanism ensures eventual success even under high concurrency. Memory context management prevents memory leaks during repeated retry attempts.