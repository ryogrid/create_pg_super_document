# mdzeroextend

## Location
[src/backend/storage/smgr/md.c:525-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L525-L636)

## Overview
Extends a relation by adding multiple zeroed blocks efficiently, using either fallocate() or zero-writing techniques depending on the number of blocks being added.

## Definition

```c
void
mdzeroextend(SMgrRelation reln, ForkNumber forknum,
			 BlockNumber blocknum, int nblocks, bool skipFsync)
```
## Detailed Description
The `mdzeroextend` function extends a relation by adding multiple blocks filled with zeroes in an optimized manner. Unlike `mdextend` which adds one block at a time, this function can add many blocks efficiently by using either `posix_fallocate()` (via `FileFallocate()`) for larger extensions or `pg_pwrite_zeros()` (via `FileZero()`) for smaller ones. The function intelligently chooses the extension method based on the number of blocks being added - using fallocate for extensions larger than 8 blocks to avoid unnecessary page cache allocation, and using zero-writing for smaller extensions to take advantage of delayed allocation on filesystems. It handles segment boundaries correctly, processing the extension in chunks that respect the RELSEG_SIZE limit.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer to the storage manager relation descriptor
- `forknum`: Fork number (MAIN_FORKNUM, FSM_FORKNUM, VM_FORKNUM, or INIT_FORKNUM) being extended
- `blocknum`: Starting block number where the extension should begin (must be >= current EOF)
- `nblocks`: Number of blocks to add (must be > 0)
- `skipFsync`: Boolean flag indicating whether to skip fsync registration for this operation

## Dependencies
- Functions called/Symbols referenced:
  - _mdfd_getseg (to get or create appropriate segments)
  - FileFallocate (for efficient large extensions using posix_fallocate)
  - FileZero (for zero-writing smaller extensions)
  - register_dirty_segment (to register for fsync if needed)
  - mdnblocks (for assertion checking in debug builds)
  - relpath (for error message generation)
  - SmgrIsTemp (to check if relation is temporary)
  - ereport (for error reporting)
  - FilePathName (for error message generation)

- Called from (representative examples):
  - Storage manager layer (via function pointer in smgr interface)

## Notes and Other Information
- Uses a cutoff of 8 blocks to decide between fallocate() and zero-writing strategies
- Prevents extending relations beyond InvalidBlockNumber to avoid overflow issues
- Processes extensions in chunks, respecting segment boundaries (RELSEG_SIZE)
- For large extensions (>8 blocks), uses `FileFallocate()` which is more efficient as it avoids kernel page cache allocation for extended pages
- For smaller extensions (≤8 blocks), uses `FileZero()` to take advantage of delayed allocation on some filesystems
- Includes comprehensive error handling with hints about checking free disk space
- Conditionally registers dirty segments for fsync unless skipFsync is true or the relation is temporary
- The function maintains proper segment boundaries and can span multiple segments if necessary
- Includes debug assertions to verify that block numbers are appropriate for extension operations
- More efficient than multiple calls to `mdextend()` when adding multiple blocks
- Part of PostgreSQL's storage manager interface, optimized for bulk relation extension operations