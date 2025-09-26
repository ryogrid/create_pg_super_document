# read_stream_get_block

## Location
[src/backend/storage/aio/read_stream.c:172-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L172-L202)

## Overview
Asks the callback function which block should be read next, managing a one-block buffer to support the unget operation.

## Definition
```c
static inline BlockNumber read_stream_get_block(ReadStream *stream, void *per_buffer_data)
```

## Detailed Description
This function serves as the primary interface for obtaining the next block number to read from a ReadStream. It first checks if there's a buffered block number available (from a previous unget operation), and if so, returns that block and clears the buffer. If no buffered block exists, it calls the stream's callback function to determine the next block to read. The function also includes Valgrind integration to mark per-buffer data as undefined for memory debugging purposes.

## Parameters / Member Variables
- `stream`: Pointer to the ReadStream structure containing callback and state information
- `per_buffer_data`: Pointer to the per-buffer data that will be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [ReadStream](../R/ReadStream.md) (structure type)
  - VALGRIND_MAKE_MEM_UNDEFINED (memory debugging macro)
  - [callback](../c/callback.md) (function pointer within stream structure)
- Called from (representative examples):
  - [read_stream_look_ahead](read_stream_look_ahead.md)
  - [read_stream_next_buffer](read_stream_next_buffer.md)
  - [read_stream_next_block](read_stream_next_block.md)

## Notes and Other Information
- This function supports the unget functionality by maintaining a one-block buffer (buffered_blocknum)
- The Valgrind integration helps detect uninitialized memory access in callback functions
- InvalidBlockNumber is used as a sentinel value to indicate no buffered block is available
- The callback function is responsible for determining which block should be read next based on the stream's access pattern