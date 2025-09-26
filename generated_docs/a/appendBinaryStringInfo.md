# appendBinaryStringInfo

## Location
[src/common/stringinfo.c:233-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L233-L258)

## Overview
A core utility function that appends arbitrary binary data to a StringInfo buffer, handling both text and binary data while ensuring proper buffer management and null termination.

## Definition
void appendBinaryStringInfo(StringInfo str, const void *data, int datalen)

## Detailed Description
appendBinaryStringInfo is a fundamental append function in PostgreSQL's StringInfo system that handles arbitrary binary data appending to an existing StringInfo buffer. Unlike other StringInfo append functions that are specialized for specific data types, this function can handle any binary data including embedded null bytes, non-text data, and unterminated strings.

The function includes an assertion to ensure the StringInfo parameter is valid, then calls enlargeStringInfo to ensure adequate buffer capacity. It uses memcpy() for efficient bulk data copying and maintains null-termination even for binary data (which may be useful for callers dealing with text that isn't null-terminated). This dual-purpose design makes it suitable for both binary data operations and text handling where the input may not be null-terminated.

## Parameters / Member Variables
- str: Target StringInfo buffer to append to
- data: Pointer to the binary data to append (can be any data type)
- datalen: Length of the data to append in bytes

## Dependencies
- Functions called/Symbols referenced:
  - enlargeStringInfo
  - memcpy (standard C library function)
  - Assert (debug assertion macro)
- Called from (representative examples):
  - appendStringInfoString
  - HandleParallelMessages
  - XLogInsertRecord
  - CopySendData
  - pq_sendbytes
  - datum_to_json_internal
  - JsonbToCStringWorker
  - range_recv
  - bytea_string_agg_transfn
  - Many other functions across PostgreSQL subsystems

## Notes and Other Information
- This is the most fundamental binary data append function in the StringInfo system
- Part of PostgreSQL's StringInfo utility system located in src/common/stringinfo.c:233-258
- Used extensively throughout PostgreSQL for network protocols, JSON processing, logging, and data serialization
- Always maintains null-termination even for binary data to support mixed text/binary use cases
- Serves as the foundation for other specialized append functions like appendStringInfoString
- Critical for PostgreSQL's client-server communication protocol and internal data handling
- Can handle embedded null bytes and arbitrary binary data safely