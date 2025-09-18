# CopySendData

## Location
src/backend/commands/copyto.c: 169 - 174

## Overview
CopySendData is a static function that appends raw binary data to the frontend message buffer during COPY TO operations, serving as the fundamental data transmission primitive for copy operations.

## Definition
```c
static void CopySendData(CopyToState cstate, const void *databuf, int datasize)
```

## Detailed Description
This function is the core data transmission function for COPY TO operations. It takes a buffer of binary data and appends it directly to the frontend message buffer without any data conversion or formatting. The function serves as a low-level building block for higher-level copy functions that format and send specific data types. Data is accumulated in the message buffer and is not immediately flushed to the client; actual transmission occurs when CopySendEndOfRow is called or when the buffer reaches capacity.

## Parameters / Member Variables
- `cstate`: Pointer to CopyToState structure containing the state information for the copy operation, including the frontend message buffer where data is accumulated
- `databuf`: Pointer to the raw data buffer containing the bytes to be sent
- `datasize`: Integer specifying the number of bytes to copy from databuf to the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - appendBinaryStringInfo (to append binary data to the message buffer)
- Called from (representative examples):
  - DR_copy (in copyto.c:120)
  - CopySendInt32 (in copyto.c:270)
  - CopySendInt16 (in copyto.c:282)
  - DoCopyTo (in copyto.c:805)
  - CopyOneRowTo (in copyto.c:966)
  - DUMPsofar macro (in copyto.c:983)

## Notes and Other Information
- No data conversion or transformation is performed by this function - it copies raw bytes
- Data is buffered and not immediately sent to the client for efficiency
- This function is static, meaning it's only accessible within the copyto.c file
- The function is used as a building block by other copy functions like CopySendString and CopySendChar
- Actual network transmission happens when the buffer is flushed, typically at row boundaries