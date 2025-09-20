# pg_lsn_ne

## Location
[src/backend/utils/adt/pg_lsn.c:127-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L127-L135)

## Overview
Compares two PostgreSQL Log Sequence Number (LSN) values for inequality, returning true if they represent different positions in the WAL (Write-Ahead Log).

## Definition

```c
Datum
pg_lsn_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the inequality operator (!=) for the pg_lsn data type in PostgreSQL. It extracts two XLogRecPtr values from the function arguments and performs a direct comparison using the C inequality operator. The function is complementary to pg_lsn_eq and is essential for SQL operations that need to determine if two LSN values reference different positions in the transaction log.

This comparison is particularly useful in replication scenarios, monitoring WAL progress, and implementing conditional logic based on LSN positions. The function enables applications to detect when LSN values have changed or diverged.

## Parameters / Member Variables
- **Argument 0**: First LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)
- **Argument 1**: Second LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN from function arguments)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from:
  - SQL queries using the != or <> operators on pg_lsn values
  - Internal PostgreSQL code requiring LSN inequality checks

## Notes and Other Information
- XLogRecPtr is internally a uint64, making the comparison straightforward and efficient
- This function is automatically invoked when using inequality operators (!=, <>) in SQL with pg_lsn operands
- The function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/pg_lsn.c:127-135