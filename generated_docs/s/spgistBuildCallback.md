# spgistBuildCallback

## Location
[src/backend/access/spgist/spginsert.c:41-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spginsert.c#L41-L72)

## Overview
A callback function used during SP-GiST index construction to process individual heap tuples, inserting them into the index.

## Definition


## Detailed Description
This static function serves as a callback for table_index_build_scan() during SP-GiST index building. It processes each heap tuple by attempting to insert it into the SP-GiST index using spgdoinsert(). The function implements retry logic to handle potential buffer-locking failures that might occur due to background writer or checkpointer activity. It works within a temporary memory context that gets reset after processing each tuple to prevent memory accumulation during the build process.

## Parameters / Member Variables
- : The SP-GiST index relation being built
- : Item pointer (tuple ID) of the heap tuple being processed
- : Array of column values from the heap tuple
- : Array of boolean flags indicating which values are NULL
- : Boolean indicating if the tuple is visible/alive
- : Build state context (cast to SpGistBuildState*)

## Dependencies
- Functions called/Symbols referenced:
  - [spgdoinsert](spgdoinsert.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - SpGistBuildState
- Called from (representative examples):
  - [spgbuild](spgbuild.md) (via table_index_build_scan)

## Notes and Other Information
The function includes retry logic to handle concurrent buffer access conflicts, even though no concurrent insertions should occur during index building. Each retry resets the temporary memory context to flush any partially built data structures.