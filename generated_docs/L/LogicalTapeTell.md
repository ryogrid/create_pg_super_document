# LogicalTapeTell

## Location
[src/backend/utils/sort/logtape.c:1162-1180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L1162-L1180)

## Overview
LogicalTapeTell returns the current position within a logical tape as a block number and offset pair, suitable for later use with LogicalTapeSeek.

## Definition
```c
void LogicalTapeTell(LogicalTape *lt, int64 *blocknum, int *offset)
```

## Detailed Description
LogicalTapeTell provides a way to capture the current read position within a logical tape, returning coordinates that can later be used with LogicalTapeSeek() to return to the exact same position. The function returns the current block number and the byte offset within that block.

The function works by:
1. Ensuring the read buffer is initialized if needed
2. Validating that the tape is using single-block buffering (required for accurate position reporting)
3. Returning the current block number and position within that block
4. The returned coordinates are suitable for random access operations

This capability is essential for sorting operations that need to mark positions for later return, such as saving positions during merge operations.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure
- `blocknum`: Output parameter that receives the current block number
- `offset`: Output parameter that receives the byte offset within the current block

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](LogicalTape.md) (structure type)
  - [ltsInitReadBuffer](../l/ltsInitReadBuffer.md) (initializes read buffer if needed)
- Called from (representative examples):
  - [tuplesort_markpos](../t/tuplesort_markpos.md) (in tuplesort.c for marking tape positions during sorting)

## Notes and Other Information
- Can be called during write phase with intention of using the position after freezing, though this usage is uncommon
- Requires buffer size to be exactly BLCKSZ (single block) to ensure position accuracy
- The returned coordinates are only valid for the specific tape they came from
- Works with both frozen and unfrozen tapes, unlike seek and backspace operations
- Essential counterpart to LogicalTapeSeek for implementing tape position save/restore functionality
- Position coordinates remain valid as long as the tape structure exists