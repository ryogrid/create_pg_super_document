# smgrextend

## Location
src/backend/storage/smgr/smgr.c: 535 - 559

## Overview
The smgrextend function adds a new block to a PostgreSQL relation file, extending the file beyond its current end-of-file (EOF) position.

## Definition


## Detailed Description
The smgrextend function is a storage manager interface for extending relation files by adding new blocks. It is specifically designed for cases where the block number is at or beyond the current EOF of the file. The function delegates the actual extension operation to the appropriate storage manager implementation through the smgrsw function pointer table. After extending the file, it updates the cached block count to maintain consistency, assuming that writing beyond EOF fills intervening space with zeroes.

## Parameters / Member Variables
- : SMgrRelation pointer identifying the relation to extend
- : ForkNumber indicating which fork of the relation to extend (main, FSM, VM, etc.)
- : BlockNumber specifying the position where the new block should be written
- : Pointer to the data buffer containing the block content to write
- : Boolean flag indicating whether to skip filesystem synchronization

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_extend (storage manager implementation function)
  - SMgrRelation (relation structure)
  - InvalidBlockNumber (constant for invalid block number)
- Called from (representative examples):
  - [_hash_alloc_buckets](../h/_hash_alloc_buckets.md) (hash index bucket allocation)
  - RelationCopyStorageUsingBuffer (relation copying utility)
  - [smgr_bulk_flush](smgr_bulk_flush.md) (bulk write operations)

## Notes and Other Information
- The function assumes that writing beyond current EOF automatically fills intervening file space with zeroes
- Updates the cached block count (smgr_cached_nblocks) optimistically, invalidating it if the expectation doesn't match
- Part of the storage manager abstraction layer, allowing different storage implementations
- Critical for relation extension operations during table growth and index expansion