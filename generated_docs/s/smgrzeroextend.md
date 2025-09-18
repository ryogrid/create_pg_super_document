# smgrzeroextend

## Location
src/backend/storage/smgr/smgr.c: 560 - 584

## Overview
The smgrzeroextend function extends a PostgreSQL relation file by adding multiple new blocks filled with zeroes in a single operation.

## Definition


## Detailed Description
The smgrzeroextend function is a storage manager interface for efficiently extending relation files by multiple blocks at once, with all added blocks being zero-filled. This function is similar to smgrextend but optimized for bulk extension operations. It delegates the actual zero-extension operation to the appropriate storage manager implementation through the smgrsw function pointer table. After extending the file, it updates the cached block count by the number of blocks added, maintaining consistency with the file system state.

## Parameters / Member Variables
- : SMgrRelation pointer identifying the relation to extend
- : ForkNumber indicating which fork of the relation to extend (main, FSM, VM, etc.)
- : BlockNumber specifying the starting position for the new blocks
- : Integer count of blocks to add to the file
- : Boolean flag indicating whether to skip filesystem synchronization

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_zeroextend (storage manager implementation function)
  - SMgrRelation (relation structure)
  - InvalidBlockNumber (constant for invalid block number)
- Called from (representative examples):
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md) (shared buffer extension)
  - ExtendBufferedRelLocal (local buffer extension)

## Notes and Other Information
- More efficient than calling smgrextend multiple times for bulk extensions
- All added blocks are guaranteed to be filled with zeroes
- Updates the cached block count optimistically by adding nblocks to the current position
- Invalidates the cache if the expected block count doesn't match actual state
- Part of the storage manager abstraction layer supporting different storage implementations
- Commonly used in buffer management for extending relations efficiently