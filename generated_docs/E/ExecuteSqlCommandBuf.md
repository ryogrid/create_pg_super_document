# ExecuteSqlCommandBuf

## Location
[src/bin/pg_dump/pg_backup_db.c:445-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L445-L499)

## Overview
Implements the ahwrite() function for direct-to-database restore operations, routing different types of data (COPY data, INSERT commands, general SQL) to appropriate handlers based on output type.

## Definition
int ExecuteSqlCommandBuf(Archive *AHX, const char *buf, size_t bufLen)

## Detailed Description
ExecuteSqlCommandBuf serves as a dispatcher function that routes incoming data buffers to the appropriate execution method based on the type of output being processed. It handles three distinct data types: COPY data (binary table data), OTHERDATA (INSERT commands and BLOB COMMENTS), and general SQL commands. For COPY data, it uses PQputCopyData to send data directly to the PostgreSQL server. For INSERT commands, it delegates to ExecuteSimpleCommands for proper parsing and execution. For general SQL commands, it ensures null-termination before passing to ExecuteSqlCommand. The function includes error handling for COPY operations and gracefully handles cases where COPY mode has failed.

## Parameters / Member Variables
- `AHX`: Archive pointer (cast to ArchiveHandle internally)
- `buf`: Buffer containing data to be written/executed
- `bufLen`: Length of the buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyData](../P/PQputCopyData.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [ExecuteSimpleCommands](ExecuteSimpleCommands.md)
  - [ExecuteSqlCommand](ExecuteSqlCommand.md)
  - pg_malloc
  - memcpy
  - free
- Constants referenced:
  - OUTPUT_COPYDATA
  - OUTPUT_OTHERDATA
- Called from (representative examples):
  - [ahwrite](../a/ahwrite.md) (in pg_backup_archiver.c)

## Notes and Other Information
- Returns the buffer length as confirmation of bytes processed
- Handles null-termination for general SQL commands when not already present
- Drops COPY data silently if libpq is not in COPY mode (error recovery behavior)
- Memory allocation and deallocation is performed when null-termination is needed
- The function serves as the main entry point for direct database restoration
- Part of the pg_backup_db.h interface as indicated by header declaration