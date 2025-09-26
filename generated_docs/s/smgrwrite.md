# smgrwrite

## Location
[src/include/storage/smgr.h:124-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/smgr.h#L124-L130)

## Overview
A lightweight inline wrapper function that writes a single block from a buffer to a storage manager relation.

## Definition
```c
static inline void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
          const void *buffer, bool skipFsync)
```

## Detailed Description
`smgrwrite` is a convenience function that provides a simplified interface for writing a single block to a storage manager relation. It internally calls `smgrwritev` (the vectorized write function) with a single buffer, effectively converting the single-block write operation into a batch operation of size 1. This design maintains consistency with the storage manager's vectorized I/O architecture while providing an easy-to-use interface for single block writes.

The function is defined as a static inline function in the header file, which means it will be inlined at compile time for optimal performance when writing individual blocks. The write operation is asynchronous - the block is not necessarily on disk at return, only dumped out to the kernel, with provisions made to fsync the write before the next checkpoint.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation to write to
- `forknum`: ForkNumber indicating which fork of the relation to write (main, FSM, VM, etc.)
- `blocknum`: BlockNumber specifying the block number to write
- `buffer`: const void pointer to the buffer containing the data to write
- `skipFsync`: bool indicating whether the caller will handle fsync separately, allowing this function to skip fsync provisions

## Dependencies
- Functions called/Symbols referenced:
  - `smgrwritev`
  - `SMgrRelation`
- Called from (representative examples):
  - `FlushBuffer`
  - `FlushRelationBuffers`
  - `GetLocalVictimBuffer`
  - `smgr_bulk_flush`

## Notes and Other Information
- This function is implemented as a static inline wrapper around `smgrwritev`
- Only for updating already-existing blocks of a relation (before current EOF); use `smgrextend` to extend relations
- The write is asynchronous - data is dumped to kernel but not necessarily written to disk immediately
- Fsync provisions are made for the next checkpoint unless `skipFsync` is true
- Temporary relations do not require fsync
- Part of PostgreSQL's storage manager interface, providing abstraction over different storage implementations