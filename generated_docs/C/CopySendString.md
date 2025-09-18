# CopySendString

## Location
src/backend/commands/copyto.c: 175 - 180

## Overview
CopySendString is a static function that appends a null-terminated string to the frontend message buffer during COPY TO operations, providing a convenient wrapper for string data transmission.

## Definition
```c
static void CopySendString(CopyToState cstate, const char *str)
```

## Detailed Description
This function is a higher-level convenience function built on top of the core CopySendData functionality. It takes a null-terminated C string and appends it to the frontend message buffer by calculating the string length and calling the underlying binary data append function. The function provides a simple interface for sending string data during copy operations without requiring the caller to manually calculate string lengths. Like CopySendData, this function buffers the data and does not immediately flush it to the client.

## Parameters / Member Variables
- `cstate`: Pointer to CopyToState structure containing the state information for the copy operation, including the frontend message buffer where the string data will be accumulated
- `str`: Pointer to a null-terminated C string to be sent to the client

## Dependencies
- Functions called/Symbols referenced:
  - appendBinaryStringInfo (to append the string data to the message buffer)
  - strlen (to calculate the length of the null-terminated string)
- Called from (representative examples):
  - DR_copy (in copyto.c:121)
  - [CopySendEndOfRow](CopySendEndOfRow.md) (in copyto.c:200)
  - [CopyOneRowTo](CopyOneRowTo.md) (in copyto.c:943)
  - [CopyAttributeOutCSV](CopyAttributeOutCSV.md) (in copyto.c:1218)

## Notes and Other Information
- The function automatically calculates string length using strlen, so the string must be null-terminated
- No data conversion or encoding transformation is performed - the string is sent as-is
- This function is static, meaning it's only accessible within the copyto.c file
- Data is buffered for efficiency and flushed later, typically at row boundaries
- The null terminator is not included in the transmitted data (only the string content is sent)