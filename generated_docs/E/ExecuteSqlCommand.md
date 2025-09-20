# ExecuteSqlCommand

## Location
[src/bin/pg_dump/pg_backup_db.c:328-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L328-L379)

## Overview
A convenience function that executes a single SQL command through PostgreSQL's libpq interface and monitors the result to detect COPY statements for pg_dump operations.

## Definition

```c
static void
ExecuteSqlCommand(ArchiveHandle *AH, const char *qry, const char *desc)
```
## Detailed Description
ExecuteSqlCommand is a utility function within pg_dump that wraps the execution of SQL queries. It sends a query to the PostgreSQL server using PQexec() and handles various result states appropriately. The function is specifically designed to work with pg_dump's archive handling system and includes special handling for COPY operations, which are commonly used during database dumps and restores. When a COPY IN result is detected, it sets the pgCopyIn flag in the ArchiveHandle to indicate that subsequent data should be treated as COPY data rather than regular query results.

## Parameters / Member Variables
- : Pointer to ArchiveHandle containing the database connection and dump context information
- : The SQL command string to execute
- : A descriptive string used in error messages to identify the operation being performed

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQclear](../P/PQclear.md)
  - [warn_or_exit_horribly](../w/warn_or_exit_horribly.md)
- Constants referenced:
  - PGRES_COMMAND_OK
  - PGRES_TUPLES_OK
  - PGRES_EMPTY_QUERY
  - PGRES_COPY_IN
- Called from (representative examples):
  - [ExecuteSimpleCommands](ExecuteSimpleCommands.md)
  - [ExecuteSqlCommandBuf](ExecuteSqlCommandBuf.md)
  - [StartTransaction](../S/StartTransaction.md)
  - [CommitTransaction](../C/CommitTransaction.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_backup_db.c file
- The function includes conditional debug output (under NOT_USED preprocessor directive) for development purposes
- Error handling is delegated to warn_or_exit_horribly, which may either warn or terminate the program depending on configuration
- The pgCopyIn flag is set when PGRES_COPY_IN is encountered, allowing the caller to handle subsequent COPY data appropriately
- Results are always cleaned up with PQclear to prevent memory leaks