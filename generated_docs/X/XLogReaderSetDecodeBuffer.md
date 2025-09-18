# XLogReaderSetDecodeBuffer

## Location
src/backend/access/transam/xlogreader.c: 90 - 105

## Overview
This function sets the size and location of the decoding buffer for an XLogReaderState, allowing caller-supplied memory to be used for WAL record decoding.

## Definition
```c
void XLogReaderSetDecodeBuffer(XLogReaderState *state, void *buffer, size_t size)
```

## Detailed Description
`XLogReaderSetDecodeBuffer` configures the decoding buffer for a WAL (Write-Ahead Log) reader. It allows callers to provide their own memory buffer that will be used for decoding non-oversized WAL records. The function initializes the buffer pointers and size in the XLogReaderState structure. This is typically called during WAL reader initialization to set up the workspace for record decoding operations.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState structure to configure
- `buffer`: Pointer to caller-supplied memory region for decoding (can be NULL)
- `size`: Size of the provided buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
- Called from (representative examples):
  - InitWalRecovery
  - XLogReaderHasQueuedRecordOrError

## Notes and Other Information
- The function asserts that no decode buffer is currently set (decode_buffer must be NULL)
- Both head and tail pointers are initially set to point to the start of the buffer
- If a NULL buffer is passed, the reader will allocate its own buffer when needed
- The buffer is used for decoding records that fit within the provided size
- Oversized records will require separate allocation regardless of this buffer setting
- This function should only be called once per XLogReaderState instance during initialization