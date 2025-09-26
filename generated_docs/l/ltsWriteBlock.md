# ltsWriteBlock

## Location
[src/backend/utils/sort/logtape.c:238-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L238-L281)

## Overview
Writes a block-sized buffer to a specified block position in the underlying BufFile of a LogicalTapeSet, handling file gaps by filling them with zeros.

## Definition
static void ltsWriteBlock(LogicalTapeSet *lts, int64 blocknum, const void *buffer)

## Detailed Description
The ltsWriteBlock function is a low-level block I/O operation within PostgreSQL's logical tape system. It writes exactly one BLCKSZ-sized block of data to a specific block position in the underlying BufFile. A key feature is its handling of "holes" in the file - since BufFile doesn't support sparse files, the function automatically fills any gap between the current end of file and the target block position with zero-filled blocks.

The function performs recursive calls to itself when filling gaps, writing zero blocks sequentially until reaching the target position. After writing the requested block, it updates the LogicalTapeSet's nBlocksWritten counter if the write extended the file.

## Parameters / Member Variables
- lts: Pointer to the LogicalTapeSet containing the target file and metadata
- blocknum: Target block number (0-based) where the data should be written  
- buffer: Pointer to the data buffer to write (must be BLCKSZ bytes)

## Dependencies
- Functions called/Symbols referenced:
  - MemSet (fills zero buffer)
  - [BufFileSeekBlock](../B/BufFileSeekBlock.md) (positions file pointer)
  - [BufFileWrite](../B/BufFileWrite.md) (performs actual write)
  - [ltsWriteBlock](ltsWriteBlock.md) (recursive calls for gap filling)
- Called from (representative examples):
  - [LogicalTapeWrite](../L/LogicalTapeWrite.md)
  - [LogicalTapeRewindForRead](../L/LogicalTapeRewindForRead.md)
  - [LogicalTapeFreeze](../L/LogicalTapeFreeze.md)

## Notes and Other Information
- Function uses recursive calls to fill file gaps, which could potentially cause stack overflow for very large gaps
- Error handling is performed via ereport() calls - no return value indicates success/failure
- The function maintains the LogicalTapeSet's nBlocksWritten counter for tracking file size
- Zero-filling behavior ensures BufFile compatibility since it doesn't support sparse files
- Block concatenation between workers can create conceptual "holes" that are tracked but never accessed