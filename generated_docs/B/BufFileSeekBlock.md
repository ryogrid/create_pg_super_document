# BufFileSeekBlock

## Location
[src/backend/storage/file/buffile.c:851-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L851-L865)

## Overview
Performs block-oriented absolute seek to the start of a specified BLCKSZ-sized block within a BufFile, providing a convenient interface for block-based I/O operations.

## Definition
```c
int BufFileSeekBlock(BufFile *file, int64 blknum)
```

## Detailed Description
BufFileSeekBlock provides a block-oriented seek interface that positions the file pointer to the beginning of the specified block number. It internally converts the block number to file number and byte offset by dividing the block number by BUFFILE_SEG_SIZE to determine which segment file contains the block, and using the remainder multiplied by BLCKSZ to find the byte offset within that file. The function is designed for applications that work with fixed-size blocks and need to seek to specific block boundaries.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure to seek within
- `blknum`: The block number to seek to (0-based), where each block is BLCKSZ bytes in size

## Dependencies
- Functions called/Symbols referenced:
  - BufFileSeek
  - BUFFILE_SEG_SIZE (constant)
  - BLCKSZ (constant)
- Called from (representative examples):
  - ReadTempFileBlock (src/backend/access/gist/gistbuildbuffers.c:752)
  - WriteTempFileBlock (src/backend/access/gist/gistbuildbuffers.c:760)
  - ltsWriteBlock (src/backend/utils/sort/logtape.c:263)
  - ltsReadBlock (src/backend/utils/sort/logtape.c:284)
  - sts_parallel_scan_next (src/backend/utils/sort/sharedtuplestore.c:545)

## Notes and Other Information
- Returns 0 on success, EOF on failure
- The logical position is not moved if an impossible seek is attempted
- Limited to files smaller than BLCKSZ * PG_INT64_MAX bytes, which is still extremely large
- Primarily used by GiST index building, log tape operations, and shared tuple store implementations
- Provides a more convenient interface than BufFileSeek when working with block-aligned data
- Each block corresponds to PostgreSQL's standard block size (BLCKSZ)