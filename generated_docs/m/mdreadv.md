# mdreadv

## Location
[src/backend/storage/smgr/md.c:810-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L810-L927)

## Overview
mdreadv performs vectored read operations on multiple blocks from a relation, handling segment boundaries, short reads, and error conditions with sophisticated retry logic.

## Definition
```c
void mdreadv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void **buffers, BlockNumber nblocks)
```

## Detailed Description
The mdreadv function implements high-performance vectored I/O for reading multiple consecutive blocks from a PostgreSQL relation. It optimizes I/O operations by using the buffers_to_iovec function to merge contiguous buffers and handles the complexity of reading across segment boundaries.

The function processes reads in chunks that respect both segment boundaries and system I/O vector limits (PG_IOV_MAX). For each chunk, it sets up iovec structures and performs the actual I/O using FileReadV. The function includes sophisticated error handling and retry logic to deal with short reads, continuing until all requested data is transferred or an unrecoverable error occurs.

Key features include distributed tracing support (TRACE_POSTGRESQL_SMGR_MD_READ_START/DONE), handling of EOF conditions, and special behavior during recovery or when zero_damaged_pages is enabled to gracefully handle missing or truncated data by zero-filling buffers.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the target relation for reading
- `forknum`: ForkNumber specifying which fork (main, FSM, VM, etc.) to read from
- `blocknum`: BlockNumber indicating the starting block number to read
- `buffers`: Array of void pointers pointing to destination buffers for the read data
- `nblocks`: BlockNumber specifying the number of consecutive blocks to read

## Dependencies
- Functions called/Symbols referenced:
  - [_mdfd_getseg](_mdfd_getseg.md) (obtains file descriptor for appropriate segment)
  - [buffers_to_iovec](../b/buffers_to_iovec.md) (converts buffer array to iovec structures)
  - [FileReadV](../F/FileReadV.md) (performs vectored file read)
  - [compute_remaining_iovec](../c/compute_remaining_iovec.md) (adjusts iovec after short reads)
  - [FilePathName](../F/FilePathName.md) (gets file path for error reporting)
  - ereport/errcode_for_file_access (error reporting)
  - TRACE_POSTGRESQL_SMGR_MD_READ_START/DONE (distributed tracing)
  - memset (zero-fills buffers when needed)
- Called from (representative examples):
  - Referenced in MD_H header file for external access

## Dependencies (Constants and Macros)
- PG_IOV_MAX (maximum number of iovec entries)
- EXTENSION_FAIL/EXTENSION_CREATE_RECOVERY (segment extension flags)
- BLCKSZ (PostgreSQL block size)
- RELSEG_SIZE (maximum blocks per segment)
- ERRCODE_DATA_CORRUPTED (error code for data corruption)

## Notes and Other Information
- Handles reads across multiple segments automatically, processing each segment separately
- Implements retry logic for short reads, continuing until all data is transferred or EOF
- During recovery or when zero_damaged_pages is enabled, gracefully handles missing data by zero-filling
- Uses vectored I/O (readv) for optimal performance when buffers are contiguous
- Includes compile-time simulation support for short reads (SIMULATE_SHORT_READ)
- Provides comprehensive error messages including block ranges and file paths
- Part of PostgreSQL's high-performance I/O subsystem with distributed tracing support
- Respects system limits on vectored I/O operations through PG_IOV_MAX checking