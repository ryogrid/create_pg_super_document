# get_per_buffer_data

## Location
[src/backend/storage/aio/read_stream.c:161-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L161-L171)

## Overview
Returns a pointer to the per-buffer data for a specific buffer index within a ReadStream's buffer pool.

## Definition

```c
static inline void *
get_per_buffer_data(ReadStream *stream, int16 buffer_index)
```
## Detailed Description
This is a utility function that calculates the memory address of per-buffer data for a given buffer index in a ReadStream. It performs pointer arithmetic to find the correct offset within the stream's per_buffer_data memory region. The function multiplies the per_buffer_data_size by the buffer_index to determine the byte offset from the base address.

## Parameters / Member Variables
- : Pointer to the ReadStream structure containing the per-buffer data array
- : The index of the buffer whose per-buffer data is being accessed

## Dependencies
- Functions called/Symbols referenced:
  - [ReadStream](../R/ReadStream.md) (structure type)
- Called from (representative examples):
  - [read_stream_look_ahead](../r/read_stream_look_ahead.md)
  - read_stream_next_buffer

## Notes and Other Information
- This is a static inline function for performance, as it's a simple pointer calculation used frequently
- The function assumes that buffer_index is valid and within the bounds of allocated buffers
- The per_buffer_data is allocated as a contiguous block, with each buffer's data stored sequentially