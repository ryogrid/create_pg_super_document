# mdextend

## Location
[src/backend/storage/smgr/md.c:460-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L460-L524)

## Overview
Extends a relation by adding a block at the specified position, typically used when writing beyond the current end-of-file of a relation fork.

## Definition

```c
void
mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 const void *buffer, bool skipFsync)
```
## Detailed Description
The `mdextend` function is responsible for extending a relation by writing a new block at the specified position. It is specifically designed for cases where the block number is at or beyond the current EOF (end-of-file). The function handles the creation of new segments if necessary, validates the block number to prevent overflow, performs the actual write operation, and manages fsync registration for durability. Unlike `mdwrite`, this function is optimized for extending relations and assumes that writing beyond EOF will fill intervening space with zeroes. It includes comprehensive error handling for disk space issues and write failures.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer to the storage manager relation descriptor
- `forknum`: Fork number (MAIN_FORKNUM, FSM_FORKNUM, VM_FORKNUM, or INIT_FORKNUM) being extended
- `blocknum`: Block number where the new block should be written (must be >= current EOF)
- `buffer`: Pointer to the block data to be written (must be I/O aligned if direct I/O is enabled)
- `skipFsync`: Boolean flag indicating whether to skip fsync registration for this write

## Dependencies
- Functions called/Symbols referenced:
  - _mdfd_getseg (to get or create the appropriate segment)
  - FileWrite (to perform the actual write operation)
  - register_dirty_segment (to register for fsync if needed)
  - mdnblocks (for assertion checking in debug builds)
  - relpath (for error message generation)
  - SmgrIsTemp (to check if relation is temporary)
  - ereport (for error reporting)
  - FilePathName (for error message generation)

- Called from (representative examples):
  - _mdfd_getseg (in some internal scenarios)
  - Storage manager layer (via function pointer in smgr interface)

## Notes and Other Information
- The function includes buffer alignment assertions when direct I/O is enabled
- Prevents extending relations beyond InvalidBlockNumber (2^32-1 blocks) to avoid wraparound issues
- Uses EXTENSION_CREATE mode when getting segments, allowing creation of new segments as needed
- Calculates file position using modulo arithmetic to handle multi-segment files correctly
- Provides detailed error messages with hints about checking free disk space
- Conditionally registers dirty segments for fsync unless skipFsync is true or the relation is temporary
- The function assumes that the underlying filesystem will zero-fill any gaps when writing beyond EOF
- Includes debug assertions to verify that block numbers are appropriate for extension operations
- Part of PostgreSQL's storage manager interface, providing the core mechanism for relation growth