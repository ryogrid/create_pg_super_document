# _hash_init

## Location
src/backend/access/hash/hashpage.c: 327 - 497

## Overview
This function initializes a new hash index by creating and setting up the metadata page, initial bucket pages, and the first bitmap page, establishing the foundational structure for hash index operations.

## Definition


## Detailed Description
 is the primary initialization function for hash indexes that performs comprehensive setup of the index structure. It calculates an appropriate number of initial buckets based on the estimated tuple count and target fill factor, then creates and initializes the metadata page, all initial bucket pages, and the first bitmap page. The function uses WAL logging when appropriate to ensure crash safety. The initialization process involves careful buffer management and follows a specific sequence to ensure the storage manager has the correct understanding of the physical index length.

## Parameters / Member Variables
- : The relation (hash index) being initialized
- : Estimated number of tuples to be loaded into the index initially
- : The fork number specifying which fork of the relation to initialize

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocksInFork (safety check for empty index)
  - RelationNeedsWAL (WAL logging determination)
  - HashGetTargetPageUsage (fill factor calculation)
  - index_getprocid (hash function procedure lookup)
  - _hash_getnewbuf (buffer allocation)
  - _hash_init_metabuffer (metadata page initialization)
  - _hash_initbuf (bucket page initialization)
  - _hash_initbitmapbuffer (bitmap page initialization)
  - _hash_relbuf (buffer release)
  - XLogInsert/XLogBeginInsert (WAL logging)
  - HashPageGetMeta (metadata page access)
  - BUCKET_TO_BLKNO (block number calculation)
  - LockBuffer/MarkBufferDirty (buffer management)

- Called from (representative examples):
  - hashbuild (index creation during BUILD)
  - hashbuildempty (empty index creation)

## Notes and Other Information
- The function performs a safety check to ensure the index is completely empty before initialization
- Calculates optimal initial bucket count based on estimated tuple count and target fill factor (minimum 10 tuples per bucket)
- WAL logs all operations when the relation is persistent or when initializing the init fork
- Uses relaxed locking rules during initialization since no concurrent access is possible
- Temporarily releases the metadata buffer lock during bucket initialization to allow interrupts and prevent blocking the background writer
- Creates the first bitmap page immediately after bucket creation and registers it in the metadata
- Returns the number of buckets created, which can be used by calling functions
- The initialization sequence (metadata → buckets → bitmap) is important for storage manager consistency
- Includes comprehensive error handling for resource limits (e.g., maximum bitmap pages)
- All buffer operations are properly WAL-logged for crash recovery when needed