# CopyReadBinaryData

## Location
src/backend/commands/copyfromparse.c: 701 - 753

## Overview
CopyReadBinaryData reads raw binary data from the input source through the buffer system, providing a fundamental building block for COPY FROM binary format operations.

## Definition


## Detailed Description
CopyReadBinaryData is a core function for binary COPY FROM operations that provides buffered reading of raw data. The function implements efficient data transfer with two main strategies:

1. **Fast path**: When the requested data is already available in the buffer (RAW_BUF_BYTES >= nbytes), it performs a simple memcpy operation to transfer the data immediately.

2. **Buffered path**: When insufficient data exists in the buffer, it enters a loop that:
   - Loads more data from the source via CopyLoadRawBuf() when the buffer is empty
   - Transfers data in chunks using Min() to handle cases where nbytes exceeds buffer size
   - Continues until the requested amount is read or EOF is reached

The function handles EOF gracefully by returning the actual number of bytes read, which may be less than requested if the input source is exhausted.

## Parameters / Member Variables
- : CopyFromState structure containing buffer state and file access information
- : Destination buffer where the read data will be copied
- : Number of bytes requested to be read
- **Returns**: Number of bytes actually read (may be less than nbytes if EOF is reached)

## Dependencies
- Functions called/Symbols referenced:
  - CopyLoadRawBuf
  - CopyFromState
- Called from (representative examples):
  - NO_END_OF_COPY_GOTO
  - ReceiveCopyBinaryHeader
  - CopyGetInt32
  - CopyGetInt16
  - NextCopyFrom
  - CopyReadBinaryAttribute

## Notes and Other Information
- The function uses RAW_BUF_BYTES macro to efficiently check available buffer data
- Memory copying is done via memcpy() for performance, assuming non-overlapping source and destination
- The loop structure ensures that requests larger than the buffer size are handled correctly by reading multiple buffer loads
- EOF detection is handled through the raw_reached_eof flag set by CopyLoadRawBuf()
- This function is used extensively throughout the binary COPY parsing pipeline for reading headers, lengths, and attribute data
- Buffer index management (raw_buf_index) is updated automatically as data is consumed