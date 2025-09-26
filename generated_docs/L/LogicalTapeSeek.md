# LogicalTapeSeek

## Location
src/backend/utils/sort/logtape.c: 1133 - 1161

## Overview
LogicalTapeSeek provides random access positioning to any arbitrary location within a frozen logical tape using previously saved block and offset coordinates.

## Definition
```c
void LogicalTapeSeek(LogicalTape *lt, int64 blocknum, int offset)
```

## Detailed Description
LogicalTapeSeek enables direct positioning to any location within a frozen logical tape. The function accepts a block number and offset that must have been previously obtained from LogicalTapeTell(). If the target block is different from the currently loaded block, it reads the new block from storage and updates the tape's internal state accordingly.

The seeking process involves:
1. Validating that the tape is frozen and parameters are valid
2. Initializing read buffer if necessary  
3. Loading the target block if different from current block
4. Setting the read position to the specified offset within the block
5. Updating internal tape state with new block metadata

This provides efficient random access for operations that need to return to previously saved positions during sorting and merging.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure (must be frozen)
- `blocknum`: Target block number (must be valid for this tape)
- `offset`: Byte offset within the target block (0 to TapeBlockPayloadSize)

## Dependencies
- Functions called/Symbols referenced:
  - LogicalTape (structure type)
  - TapeBlockPayloadSize (constant for maximum valid offset)
  - ltsInitReadBuffer (initializes read buffer if needed)
  - ltsReadBlock (reads target block from storage)
  - TapeBlockGetTrailer (accesses block metadata for next block pointer)
- Called from (representative examples):
  - tuplesort_restorepos (in tuplesort.c for restoring saved tape positions)

## Notes and Other Information
- Only works on frozen tapes - seeking is not supported during write operations or on unfrozen read tapes
- Requires buffer size to be exactly BLCKSZ (single block) for proper operation
- Block number and offset parameters must have been obtained from a previous LogicalTapeTell() call
- Validates offset is within valid range (0 to TapeBlockPayloadSize)
- Optimized to avoid unnecessary block reads when seeking within the currently loaded block
- Essential for tuple sorting operations that need to restore previously saved tape positions
- Throws error for invalid seek positions beyond block boundaries