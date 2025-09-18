# ExecuteSqlStatement

## Location
[src/bin/pg_dump/pg_backup_db.c:278-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L278-L289)

## Overview
A utility function in pg_dump that executes SQL statements that do not return result sets, with automatic error handling and cleanup.

## Definition
```c
void ExecuteSqlStatement(Archive *AHX, const char *query)
```

## Detailed Description
The `ExecuteSqlStatement` function is a core utility function used throughout pg_dump to execute SQL statements that are expected to succeed and do not need to return data (such as DDL statements, SET commands, or other administrative SQL). The function encapsulates the common pattern of executing a query via libpq, checking for successful completion, and cleaning up resources.

The function specifically checks that the query result status is `PGRES_COMMAND_OK`, which indicates successful execution of a command that does not return rows. If the query fails for any reason, the function calls `die_on_query_failure` to report the error and terminate the program, making this function suitable for scenarios where query failure is considered a fatal condition.

This function is widely used throughout the pg_dump codebase for executing setup queries, configuration changes, and schema definition statements where failure should halt the dump process.

## Parameters / Member Variables
- `AHX`: A pointer to an Archive structure (cast to ArchiveHandle internally) that contains the database connection and other pg_dump context
- `query`: A null-terminated string containing the SQL statement to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQclear](../P/PQclear.md)
  - [die_on_query_failure](../d/die_on_query_failure.md)
  - PGRES_COMMAND_OK (constant)

- Called from (representative examples):
  - [setup_connection](../s/setup_connection.md) (multiple times)
  - [expand_table_name_patterns](../e/expand_table_name_patterns.md)
  - [dumpTableData_insert](../d/dumpTableData_insert.md)
  - [getTables](../g/getTables.md)
  - [dumpEnumType](../d/dumpEnumType.md)
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpTable](../d/dumpTable.md)

## Notes and Other Information
- This function is part of the public API for pg_dump modules, declared in pg_backup_db.h
- The function performs automatic resource cleanup by calling PQclear on the result
- It is designed for SQL statements that do not return data - for queries that return rows, use ExecuteSqlQuery or ExecuteSqlQueryForSingleRow instead
- The function uses a fatal error handling approach - any query failure terminates the entire pg_dump process
- The Archive parameter uses type punning (casting from Archive* to ArchiveHandle*) to maintain API compatibility while accessing internal connection details