# XLogReaderFree

## Location
[src/backend/access/transam/xlogreader.c:161-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L161-L189)

## Overview
This function deallocates an XLogReaderState structure and all its associated memory resources, properly closing any open WAL segment files.

## Definition
```c
void XLogReaderFree(XLogReaderState *state)
```

## Detailed Description
`XLogReaderFree` performs a complete cleanup of an XLogReaderState structure by freeing all allocated memory and closing any open file handles. The function first closes any open WAL segment file using the callback routine, then conditionally frees the decode buffer if it was allocated by the reader itself, and finally deallocates all other memory buffers including the error message buffer, record buffer, read buffer, and the state structure itself. This is the complementary function to XLogReaderAllocate.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - segment_close (callback function via state->routine)
- Called from (representative examples):
  - [ShutdownWalRecovery](../S/ShutdownWalRecovery.md)
  - [FreeDecodingContext](../F/FreeDecodingContext.md)
  - [SummarizeWAL](../S/SummarizeWAL.md)
  - [main](../m/main.md) (pg_waldump utility)
  - [XlogReadTwoPhaseData](XlogReadTwoPhaseData.md)
  - [extractPageMap](../e/extractPageMap.md)

## Notes and Other Information
- The function safely handles the case where ws_file is -1 (no open file)
- Only frees the decode buffer if free_decode_buffer flag is set, indicating it was allocated by the reader
- Handles NULL readRecordBuf gracefully (it may not be allocated in some cases)  
- The function assumes all pointers in the state structure are either valid or NULL
- Must be called to prevent memory leaks when done with a WAL reader
- Should not be called on the same state structure more than once
- No return value - this is a void function that performs cleanup only

## Simplified Source

```c
// Simplified version of XLogReaderFree
void XLogReaderFree(XLogReaderState *state) {
    // Close any open WAL segment file
    if (state->seg.ws_file != -1)
        state->routine.segment_close(state);

    // Free decode buffer if allocated by reader
    if (state->decode_buffer && state->free_decode_buffer)
        pfree(state->decode_buffer);

    // Free all other allocated buffers
    pfree(state->errormsg_buf);
    if (state->readRecordBuf)
        pfree(state->readRecordBuf);
    pfree(state->readBuf);
    pfree(state);
}
```

Key simplifications made:
- Function is already well-structured for cleanup
- Handles conditional cleanup based on allocation flags
- Systematically frees all allocated resources