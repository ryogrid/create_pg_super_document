# LogicalTapeBackspace

## Location
[src/backend/utils/sort/logtape.c:1062-1132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L1062-L1132)

## Overview
LogicalTapeBackspace moves the read position backward on a frozen logical tape by a specified number of bytes, enabling backward traversal of tape contents during merge operations.

## Definition
```c
size_t LogicalTapeBackspace(LogicalTape *lt, size_t size)
```

## Detailed Description
LogicalTapeBackspace provides backward seek capability on frozen logical tapes by moving the current read position backward by the requested number of bytes. The function handles two scenarios: simple backspace within the current block, and complex backspace that requires walking backward through the chain of tape blocks.

The function works by:
1. For small backspaces within the current block, simply adjusting the position
2. For larger backspaces, walking backward through the block chain using block trailer links
3. Reading previous blocks as needed to reach the target position
4. Validating block chain integrity during traversal
5. Returning the actual number of bytes backed up (may be less than requested if hitting tape beginning)

This capability is essential for tuple sorting operations that need to re-examine previously read tuples during merge phases.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure (must be frozen)
- `size`: Number of bytes to backspace from current position

## Dependencies
- Functions called/Symbols referenced:
  - LogicalTape (structure type)
  - ltsInitReadBuffer (initializes read buffer if needed)
  - TapeBlockGetTrailer (accesses block metadata)
  - ltsReadBlock (reads blocks from storage)
  - TapeBlockPayloadSize (constant for block data size)
- Called from (representative examples):
  - tuplesort_gettuple_common (in tuplesort.c for tuple retrieval during merges)

## Notes and Other Information
- Only works on frozen tapes - random access is not supported during write operations or on unfrozen read tapes
- Requires buffer size to be exactly BLCKSZ (single block) for proper operation
- Returns actual bytes backed up, which may be less than requested if reaching tape beginning
- Validates block chain integrity during backward traversal, throwing errors for broken chains
- Implementation is optimized for small seeks (typical: backing up over one tuple) rather than long backward jumps
- Positions tape at beginning if backspace request exceeds available data
- Essential for merge operations that need to re-examine tuples