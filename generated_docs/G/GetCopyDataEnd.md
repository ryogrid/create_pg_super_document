# GetCopyDataEnd

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1569 - 1583

## Overview
GetCopyDataEnd is a static utility function in pg_basebackup that validates whether a COPY data message was completely parsed by checking if the cursor position matches the message length.

## Definition


## Detailed Description
This function serves as a validation mechanism to ensure that streaming replication COPY data messages are fully parsed. It compares the total message length (r) against the current parsing position (cursor). If these values don't match, it indicates that the parsing process didn't consume the entire message, which suggests a parsing error or corrupted data. When such a mismatch is detected, the function calls ReportCopyDataParseError to handle the error condition appropriately.

The function is designed as a simple but critical safety check in the pg_basebackup utility's data processing pipeline, helping to maintain data integrity during base backup operations.

## Parameters / Member Variables
- : The total length of the received COPY data message in bytes
- : Pointer to the buffer containing the COPY data message (used for error reporting)
- : Current position/offset within the message after parsing, indicating how many bytes were consumed

## Dependencies
- Functions called/Symbols referenced:
  - [ReportCopyDataParseError](../R/ReportCopyDataParseError.md)
- Called from (representative examples):
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md) (multiple locations in pg_basebackup.c)

## Notes and Other Information
- This is a static function, so it's only accessible within the pg_basebackup.c compilation unit
- The function is specifically used in archive streaming contexts during base backup operations
- It acts as a defensive programming practice to catch potential parsing inconsistencies early
- The function is called at multiple points in ReceiveArchiveStreamChunk, indicating its importance in validating different types of archive stream data