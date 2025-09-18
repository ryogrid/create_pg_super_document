# ReportCopyDataParseError

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1584 - 1598

## Overview
ReportCopyDataParseError is a static error reporting function in pg_basebackup that provides diagnostic information about malformed COPY data messages and terminates the program.

## Definition
```c
static void ReportCopyDataParseError(size_t r, char *copybuf)
```

## Detailed Description
This function serves as a centralized error handler for COPY data parsing failures during base backup operations. When called, it analyzes the problematic message and provides diagnostic information to help identify the nature of the parsing error. The function distinguishes between empty messages (length 0) and malformed messages, providing different error messages for each case.

For malformed messages, it reports both the message type (extracted from the first byte of the buffer) and the total message length. This debugging information can be valuable for troubleshooting issues with the streaming replication protocol or network corruption. After reporting the error, the function calls pg_fatal() which terminates the program execution.

The function is designed as a 'can't-happen' case handler, as noted in the comments - under normal circumstances, COPY messages should always be well-formed when received from the PostgreSQL server.

## Parameters / Member Variables
- `r`: The total length of the received COPY data message in bytes
- `copybuf`: Pointer to the buffer containing the malformed COPY data message, used to extract the message type from the first byte

## Dependencies
- Functions called/Symbols referenced:
  - pg_fatal (implicitly called, terminates program)
- Called from (representative examples):
  - GetCopyDataEnd
  - GetCopyDataByte
  - GetCopyDataString
  - GetCopyDataUInt64
  - ReceiveArchiveStreamChunk

## Notes and Other Information
- This is a static function, only accessible within the pg_basebackup.c compilation unit
- The function always terminates the program - it never returns to the caller
- It serves as a debugging aid by providing message type and length information
- The error messages help distinguish between completely empty messages and messages with invalid content
- Used throughout the COPY data parsing infrastructure in pg_basebackup for consistent error handling