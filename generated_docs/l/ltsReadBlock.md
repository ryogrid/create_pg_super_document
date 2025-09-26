# ltsReadBlock

## Location
[src/backend/utils/sort/logtape.c:282-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L282-L297)

## Overview
Reads a block-sized buffer from a specified block position in the underlying BufFile of a LogicalTapeSet.

## Definition
static void ltsReadBlock(LogicalTapeSet *lts, int64 blocknum, void *buffer)

## Detailed Description
The ltsReadBlock function is a low-level block I/O operation within PostgreSQL's logical tape system that performs direct block reads from the underlying BufFile. It is the counterpart to ltsWriteBlock, providing simple block-level access to previously written data. The function assumes the caller knows that the requested block exists and contains valid data.

Unlike ltsWriteBlock, this function is straightforward - it seeks to the specified block position and reads exactly BLCKSZ bytes into the provided buffer. The function uses BufFileReadExact to ensure the full block is read, which will error if insufficient data is available.

## Parameters / Member Variables
- lts: Pointer to the LogicalTapeSet containing the target file
- blocknum: Target block number (0-based) to read from
- buffer: Pointer to buffer where the read data will be stored (must accommodate BLCKSZ bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileSeekBlock](../B/BufFileSeekBlock.md) (positions file pointer to target block)
  - [BufFileReadExact](../B/BufFileReadExact.md) (reads exactly BLCKSZ bytes)
- Called from (representative examples):
  - [ltsReadFillBuffer](ltsReadFillBuffer.md)
  - [LogicalTapeFreeze](../L/LogicalTapeFreeze.md)
  - [LogicalTapeBackspace](../L/LogicalTapeBackspace.md)
  - [LogicalTapeSeek](../L/LogicalTapeSeek.md)

## Notes and Other Information
- Function assumes the caller has verified the block exists and is readable
- Uses BufFileReadExact which will ereport() if unable to read the full BLCKSZ
- No return value - errors are handled via ereport() calls
- Simpler than ltsWriteBlock as it doesn't need to handle gaps or update counters
- Critical for tape rewinding and seeking operations in the logical tape system