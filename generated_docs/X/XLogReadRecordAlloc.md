# XLogReadRecordAlloc

## Location
[src/backend/access/transam/xlogreader.c:438-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L438-L527)

## Overview
Allocates space for a decoded WAL (Write-Ahead Log) record within a circular decode buffer, with fallback to oversized allocation when needed.

## Definition

```c
static DecodedXLogRecord *
XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized)
```
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

## Simplified Source

```c
static DecodedXLogRecord *XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized) {
    size_t required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);
    DecodedXLogRecord *decoded = NULL;

    // Initialize circular decode buffer if needed
    if (unlikely(state->decode_buffer == NULL)) {
        if (state->decode_buffer_size == 0)
            state->decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
        state->decode_buffer = palloc(state->decode_buffer_size);
        state->decode_buffer_head = state->decode_buffer;
        state->decode_buffer_tail = state->decode_buffer;
        state->free_decode_buffer = true;
    }

    // Try to allocate in circular buffer
    if (state->decode_buffer_tail >= state->decode_buffer_head) {
        // Case 1: Try space after tail
        if (required_space <= state->decode_buffer_size - (state->decode_buffer_tail - state->decode_buffer)) {
            decoded = (DecodedXLogRecord *) state->decode_buffer_tail;
            decoded->oversized = false;
            return decoded;
        }
        // Case 2: Try space before head (wrap around)
        else if (required_space < state->decode_buffer_head - state->decode_buffer) {
            decoded = (DecodedXLogRecord *) state->decode_buffer;
            decoded->oversized = false;
            return decoded;
        }
    } else {
        // Case 3: Tail is left of head - try space between them
        if (required_space < state->decode_buffer_head - state->decode_buffer_tail) {
            decoded = (DecodedXLogRecord *) state->decode_buffer_tail;
            decoded->oversized = false;
            return decoded;
        }
    }

    // No space in buffer - allocate oversized if allowed
    if (allow_oversized) {
        decoded = palloc(required_space);
        decoded->oversized = true;
        return decoded;
    }

    return NULL;
}
```