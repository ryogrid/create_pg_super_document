# die_on_query_failure

## Location
[src/bin/pg_dump/pg_backup_db.c:269-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L269-L277)

## Overview
A static error handling function in pg_dump that logs query failure details and terminates the program execution with an error status.

## Definition
```c
static void die_on_query_failure(ArchiveHandle *AH, const char *query)
```

## Detailed Description
The `die_on_query_failure` function is a specialized error handling routine used within pg_dump when SQL query execution fails. It serves as a centralized way to report query failures with comprehensive error information before terminating the program. The function logs both the PostgreSQL server error message (obtained from the database connection) and the actual SQL query that failed, providing developers and users with complete context for debugging query-related issues.

This function is designed as a fatal error handler - once called, it will always terminate the program with exit code 1, making it suitable for scenarios where query failure represents an unrecoverable condition in the pg_dump process.

## Parameters / Member Variables
- `AH`: A pointer to an ArchiveHandle structure that contains the database connection and other pg_dump context information
- `query`: A null-terminated string containing the SQL query that failed to execute

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_error (implicitly called for error message)
  - pg_log_error_detail
  - [PQerrorMessage](../P/PQerrorMessage.md) (implicitly called to get connection error)
  - exit

- Called from (representative examples):
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_backup_db.c compilation unit
- The function never returns - it always calls exit(1) to terminate the program
- It provides a two-level error reporting: the main error message shows the database connection error, while the detail shows the actual query
- This function is specifically designed for pg_dump context and relies on the ArchiveHandle structure for database connection access
- The error output follows PostgreSQL's standard error message format with primary message and detail lines

## Simplified Source

```c
static void die_on_query_failure(ArchiveHandle *AH, const char *query)
{
    // Log database error and the failed query
    pg_log_error("query failed: %s", PQerrorMessage(AH->connection));
    pg_log_error_detail("Query was: %s", query);

    // Exit with error status
    exit(1);
}
```