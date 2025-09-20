# smgrwritev

## Location
[src/backend/storage/smgr/smgr.c:631-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L631-L642)

## Overview
The smgrwritev function performs vectorized write operations on existing blocks of a PostgreSQL relation, writing multiple buffers in a single operation.

## Definition

```c
void
smgrwritev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void **buffers, BlockNumber nblocks, bool skipFsync)
```
## Detailed Description
The smgrwritev function is a storage manager interface for performing vectorized write operations on relation files. It writes multiple buffers to consecutive blocks starting from a specified block number in a single call. This function is specifically designed for updating already-existing blocks of a relation (those before the current EOF) and should not be used for extending relations - use smgrextend() for that purpose. The writes are asynchronous and not immediately synchronized to disk, but provisions are made to fsync before the next checkpoint. The function includes mechanisms to prevent race conditions with concurrent checkpoints through buffer locking or redo pointer checking.

## Parameters / Member Variables
- : SMgrRelation pointer identifying the relation to write to
- : ForkNumber indicating which fork of the relation to write (main, FSM, VM, etc.)
- : BlockNumber specifying the starting block position for writing
- : Array of const void pointers containing the data to write
- : BlockNumber indicating the count of consecutive blocks to write
- : Boolean flag indicating whether the caller will handle fsync separately

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_writev (storage manager implementation function)
  - SMgrRelation (relation structure)
- Called from (representative examples):
  - smgrwrite (single block write wrapper function)

## Notes and Other Information
- Only for updating existing blocks, not for relation extension (use smgrextend for that)
- Asynchronous write operation - data may not be immediately on disk
- Fsync is scheduled for the next checkpoint unless skipFsync is true
- Race condition prevention through buffer locking or redo pointer mechanisms
- Temporary relations do not require fsync operations
- Part of the storage manager abstraction layer
- More efficient than multiple individual smgrwrite calls for consecutive blocks
- Critical for bulk write operations and buffer management optimization