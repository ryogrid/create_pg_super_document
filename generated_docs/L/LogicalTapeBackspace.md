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
  - [LogicalTape](LogicalTape.md) (structure type)
  - [ltsInitReadBuffer](../l/ltsInitReadBuffer.md) (initializes read buffer if needed)
  - TapeBlockGetTrailer (accesses block metadata)
  - [ltsReadBlock](../l/ltsReadBlock.md) (reads blocks from storage)
  - TapeBlockPayloadSize (constant for block data size)
- Called from (representative examples):
  - [tuplesort_gettuple_common](../t/tuplesort_gettuple_common.md) (in tuplesort.c for tuple retrieval during merges)

## Notes and Other Information
- Only works on frozen tapes - random access is not supported during write operations or on unfrozen read tapes
- Requires buffer size to be exactly BLCKSZ (single block) for proper operation
- Returns actual bytes backed up, which may be less than requested if reaching tape beginning
- Validates block chain integrity during backward traversal, throwing errors for broken chains
- Implementation is optimized for small seeks (typical: backing up over one tuple) rather than long backward jumps
- Positions tape at beginning if backspace request exceeds available data
- Essential for merge operations that need to re-examine tuples

## Simplified Source

```c
size_t LogicalTapeBackspace(LogicalTape *lt, size_t size) {
    size_t seekpos = 0;

    // Verify tape is frozen and ready for reading
    Assert(lt->frozen);
    Assert(lt->buffer_size == BLCKSZ);

    // Initialize read buffer if needed
    if (lt->buffer == NULL) {
        ltsInitReadBuffer(lt);
    }

    // Easy case: backspace within current block
    if (size <= (size_t) lt->pos) {
        lt->pos -= (int) size;
        return size;
    }

    // Complex case: walk backward through block chain
    seekpos = (size_t) lt->pos;  // bytes available in current block

    while (size > seekpos) {
        // Get previous block number from current block trailer
        int64 prev = TapeBlockGetTrailer(lt->buffer)->prev;

        // Check if we've reached the beginning of tape
        if (prev == -1L) {
            if (lt->curBlockNumber != lt->firstBlockNumber) {
                elog(ERROR, "unexpected end of tape");
            }
            lt->pos = 0;
            return seekpos;  // return partial backspace
        }

        // Read the previous block
        ltsReadBlock(lt->tapeSet, prev, lt->buffer);

        // Validate block chain integrity
        if (TapeBlockGetTrailer(lt->buffer)->next != lt->curBlockNumber) {
            elog(ERROR, "broken tape, next of block %lld is %lld, expected %lld",
                 (long long) prev,
                 (long long) (TapeBlockGetTrailer(lt->buffer)->next),
                 (long long) lt->curBlockNumber);
        }

        // Update tape position to the previous block
        lt->nbytes = TapeBlockPayloadSize;
        lt->curBlockNumber = prev;
        lt->nextBlockNumber = TapeBlockGetTrailer(lt->buffer)->next;

        seekpos += TapeBlockPayloadSize;
    }

    // Calculate final position within the target block
    lt->pos = seekpos - size;
    return size;
}
```