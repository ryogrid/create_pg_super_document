# GetCopyDataString

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1530 - 1553

## Overview
Extracts a NUL-terminated string from a COPY data message buffer and advances the cursor past the string.

## Definition


## Detailed Description
GetCopyDataString is a utility function that safely reads a NUL-terminated string from a COPY protocol message buffer. It scans the buffer starting from the current cursor position to find the terminating NUL byte (\0), ensuring that the search does not exceed the buffer boundaries. Upon finding the NUL terminator, the function updates the cursor to point to the position immediately after the string and returns a pointer to the beginning of the string within the buffer.

This function is essential for parsing string fields in COPY protocol messages, such as archive names and tablespace locations during base backup operations.

## Parameters / Member Variables
- : Total size of the data buffer in bytes
- : Pointer to the buffer containing the COPY data message
- : Pointer to the current position within the buffer, updated to point past the string after reading

## Dependencies
- Functions called/Symbols referenced:
  - ReportCopyDataParseError
- Called from (representative examples):
  - ReceiveArchiveStreamChunk
  - CompressionLocation

## Notes and Other Information
- The function performs bounds checking to prevent buffer overrun during string scanning
- Returns a pointer directly into the original buffer rather than allocating new memory
- The cursor is advanced past the NUL terminator, positioning it for the next read operation
- If no NUL terminator is found within the buffer bounds, the function calls ReportCopyDataParseError and does not return
- This is a static utility function only used within pg_basebackup.c for COPY protocol parsing
- The returned string pointer is valid only as long as the original copybuf remains valid