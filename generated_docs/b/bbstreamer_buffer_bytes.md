# bbstreamer_buffer_bytes

## Location
[src/bin/pg_basebackup/bbstreamer.h:157-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer.h#L157-L174)

## Overview
This convenience function appends a specified number of bytes from incoming data to a bbstreamer's internal buffer and adjusts the data pointer and length accordingly.

## Definition

```c
static inline void
bbstreamer_buffer_bytes(bbstreamer *streamer, const char **data, int *len,
						int nbytes)
```
## Detailed Description
bbstreamer_buffer_bytes is a static inline convenience function designed specifically for use by bbstreamer implementations, not external callers. It provides a standardized way to buffer incoming data by appending a specified number of bytes to the streamer's internal buffer (bbs_buffer) and then updating the input parameters to reflect the consumed data.

The function takes a portion of the input data (specified by nbytes), appends it to the streamer's internal StringInfo buffer using appendBinaryStringInfo, and then advances the data pointer while reducing the length counter. This pattern is common in streaming data processing where partial data needs to be accumulated before it can be processed.

## Parameters / Member Variables
- : Pointer to the bbstreamer object whose buffer will receive the data
- : Pointer to a pointer to the input data buffer (modified to advance past buffered bytes)  
- : Pointer to the length of remaining input data (modified to reflect consumed bytes)
- : Number of bytes to copy from the input data to the internal buffer

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (struct type)
  - appendBinaryStringInfo (PostgreSQL StringInfo function)
  - Assert (assertion macro)

- Called from (representative examples):
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md)
  - [bbstreamer_tar_parser_content](bbstreamer_tar_parser_content.md)

## Notes and Other Information
- This is a static inline function defined in bbstreamer.h for internal use by bbstreamer implementations
- Not intended for use by external callers - it's a utility function for bbstreamer authors
- Uses PostgreSQL's StringInfo infrastructure for dynamic buffer management
- The Assert ensures that the requested number of bytes doesn't exceed available data
- Modifies the input parameters by reference, making it easy to process data in chunks
- Common pattern in streaming parsers where headers or partial data must be accumulated before processing
- The function safely handles binary data through appendBinaryStringInfo rather than string operations