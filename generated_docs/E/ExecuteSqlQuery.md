# ExecuteSqlQuery

## Location
[src/bin/pg_dump/pg_backup_db.c:290-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L290-L304)

## Overview
A utility function in pg_dump that executes SQL queries expected to return result sets, with automatic error handling and status verification.

## Definition
```c
PGresult *ExecuteSqlQuery(Archive *AHX, const char *query, ExecStatusType status)
```

## Detailed Description
The `ExecuteSqlQuery` function is a fundamental utility function used throughout pg_dump to execute SQL queries that return data (such as SELECT statements) while providing centralized error handling. Unlike `ExecuteSqlStatement`, this function returns the PGresult pointer to the caller, allowing them to process the returned data.

The function accepts an expected status parameter, allowing callers to specify what constitutes successful execution (typically `PGRES_TUPLES_OK` for SELECT queries). This flexibility enables the function to be used for different types of queries that may have different success criteria. If the actual result status does not match the expected status, the function calls `die_on_query_failure` to report the error and terminate the program.

The caller is responsible for calling `PQclear()` on the returned result to free the memory, as this function does not perform automatic cleanup like `ExecuteSqlStatement` does.

## Parameters / Member Variables
- `AHX`: A pointer to an Archive structure (cast to ArchiveHandle internally) that contains the database connection and other pg_dump context
- `query`: A null-terminated string containing the SQL query to execute
- `status`: The expected ExecStatusType result status for successful execution (e.g., PGRES_TUPLES_OK)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [die_on_query_failure](../d/die_on_query_failure.md)
  - ExecStatusType (type)

- Called from (representative examples):
  - [ExecuteSqlQueryForSingleRow](ExecuteSqlQueryForSingleRow.md)
  - [expand_schema_name_patterns](../e/expand_schema_name_patterns.md)
  - [dumpTableData_copy](../d/dumpTableData_copy.md)
  - [getNamespaces](../g/getNamespaces.md)
  - [getTables](../g/getTables.md)
  - [getConstraints](../g/getConstraints.md)
  - [dumpDatabase](../d/dumpDatabase.md)

## Notes and Other Information
- This function is part of the public API for pg_dump modules, declared in pg_backup_db.h
- Unlike ExecuteSqlStatement, this function does NOT automatically clean up the PGresult - the caller must call PQclear()
- The function is widely used throughout pg_dump for data retrieval operations
- The status parameter provides flexibility to handle different query types with different expected success statuses
- This function uses fatal error handling - any query failure or unexpected status terminates the entire pg_dump process
- The Archive parameter uses type punning (casting from Archive* to ArchiveHandle*) for internal access while maintaining API compatibility

## Simplified Source

```c
PGresult *
ExecuteSqlQuery(Archive *AHX, const char *query, ExecStatusType status)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;
    PGresult *res;

    // Execute the SQL query
    res = PQexec(AH->connection, query);

    // Verify expected result status
    if (PQresultStatus(res) != status)
        die_on_query_failure(AH, query);

    // Return result to caller (caller must PQclear)
    return res;
}
```