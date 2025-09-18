# XLogReadRecordAlloc

## Location
src/backend/access/transam/xlogreader.c: 438 - 527

## Overview
Allocates space for a decoded WAL (Write-Ahead Log) record within a circular decode buffer, with fallback to oversized allocation when needed.

## Definition


## Detailed Description
This function manages memory allocation for decoded XLog records using a circular buffer strategy. It attempts to allocate space within the existing decode buffer first, and if that fails, can optionally allocate oversized memory outside the buffer. The function implements sophisticated circular buffer logic to efficiently reuse memory space.

The circular buffer allocation works by tracking head and tail pointers and handles three scenarios:
1. When tail >= head: tries space after tail, then before head
2. When tail < head: tries space between tail and head  
3. When no space in buffer: allocates oversized memory if allowed

Only the 'oversized' flag is initialized in the returned record, leaving other initialization to the caller. This design allows for easy cleanup if decoding fails.

## Parameters / Member Variables
- : Pointer to XLogReaderState containing the decode buffer and related metadata
- : Total length of the XLog record to be decoded
- : Boolean flag indicating whether oversized allocation is permitted when buffer space is insufficient

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeXLogRecordRequiredSpace](../D/DecodeXLogRecordRequiredSpace.md)
  - [palloc](../p/palloc.md)
  - DEFAULT_DECODE_BUFFER_SIZE
- Called from (representative examples):
  - [XLogDecodeNextRecord](XLogDecodeNextRecord.md)

## Notes and Other Information
- Returns NULL if no space is available and oversized allocation is not allowed, or if memory allocation fails
- The caller must adjust decode_buffer_tail with the actual size after successful decoding
- Initializes circular decode buffer on first use with DEFAULT_DECODE_BUFFER_SIZE
- Uses unlikely() macro for performance optimization on buffer initialization path
- Memory allocated with oversized=true must be explicitly freed with pfree()