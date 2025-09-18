# ExecuteSqlCommand

## Location
src/bin/pg_dump/pg_backup_db.c: 328 - 379

## Overview
A convenience function that executes a single SQL command through PostgreSQL's libpq interface and monitors the result to detect COPY statements for pg_dump operations.

## Definition


## Detailed Description
ExecuteSqlCommand is a utility function within pg_dump that wraps the execution of SQL queries. It sends a query to the PostgreSQL server using PQexec() and handles various result states appropriately. The function is specifically designed to work with pg_dump's archive handling system and includes special handling for COPY operations, which are commonly used during database dumps and restores. When a COPY IN result is detected, it sets the pgCopyIn flag in the ArchiveHandle to indicate that subsequent data should be treated as COPY data rather than regular query results.

## Parameters / Member Variables
- : Pointer to ArchiveHandle containing the database connection and dump context information
- : The SQL command string to execute
- : A descriptive string used in error messages to identify the operation being performed

## Dependencies
- Functions called/Symbols referenced:
  - PQexec
  - PQresultStatus
  - PQerrorMessage
  - PQclear
  - warn_or_exit_horribly
- Constants referenced:
  - PGRES_COMMAND_OK
  - PGRES_TUPLES_OK
  - PGRES_EMPTY_QUERY
  - PGRES_COPY_IN
- Called from (representative examples):
  - ExecuteSimpleCommands
  - ExecuteSqlCommandBuf
  - StartTransaction
  - CommitTransaction

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_backup_db.c file
- The function includes conditional debug output (under NOT_USED preprocessor directive) for development purposes
- Error handling is delegated to warn_or_exit_horribly, which may either warn or terminate the program depending on configuration
- The pgCopyIn flag is set when PGRES_COPY_IN is encountered, allowing the caller to handle subsequent COPY data appropriately
- Results are always cleaned up with PQclear to prevent memory leaks