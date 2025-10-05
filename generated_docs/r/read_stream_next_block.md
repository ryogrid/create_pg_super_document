# read_stream_next_block

## Location
[src/backend/storage/aio/read_stream.c:784-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L784-L796)

## Overview
Provides transitional support for code that wants to obtain the next block number from a read stream's lookahead algorithm without actually reading the buffer.

## Definition

```c
BlockNumber
read_stream_next_block(ReadStream *stream, BufferAccessStrategy *strategy)
```
## Detailed Description
This function serves as a transitional interface for legacy code that needs to know which block the read stream would read next, but wants to handle the actual buffer reading itself. It consumes the next block number from the stream's internal lookahead algorithm and returns the associated buffer access strategy that would be used for reading.

The function is designed to support gradual migration of code from manual buffer reading to the full read stream interface, allowing developers to leverage the stream's intelligent block prediction while maintaining control over the actual I/O operations.

## Parameters / Member Variables
- `*stream`: The read stream object containing the lookahead state
- `*strategy`: Output parameter that receives the buffer access strategy for the returned block
## Dependencies
- Functions called/Symbols referenced:
  - [read_stream_get_block](read_stream_get_block.md)
- Called from (representative examples):
  - (No current callers - transitional interface)

## Notes and Other Information
- Intended as a transitional API for code migrating to full read stream usage
- Consumes the block from the stream's lookahead queue, advancing the internal state
- Returns InvalidBlockNumber when the end of the stream is reached
- The strategy parameter receives the same BufferAccessStrategy that was passed to read_stream_begin_relation
- Does not perform any actual I/O - only provides the block number and strategy for external use

## Simplified Source

```c
BlockNumber read_stream_next_block(ReadStream *stream, BufferAccessStrategy *strategy) {
    // Set the output strategy parameter from the stream's current strategy
    *strategy = stream->ios[0].op.strategy;

    // Get and consume the next block number from the stream's lookahead algorithm
    return read_stream_get_block(stream, NULL);
}
```

**Key Logic:**
- Retrieves the buffer access strategy from the stream's internal state
- Delegates to `read_stream_get_block()` to consume and return the next predicted block
- Returns `InvalidBlockNumber` when stream is exhausted
- Transitional interface for legacy code migrating to read streams