# pg_lsn_eq

## Location
[src/backend/utils/adt/pg_lsn.c:118-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L118-L126)

## Overview
Compares two PostgreSQL Log Sequence Number (LSN) values for equality, returning true if they represent the same position in the WAL (Write-Ahead Log).

## Definition


## Detailed Description
This function implements the equality operator (=) for the pg_lsn data type in PostgreSQL. It extracts two XLogRecPtr values from the function arguments and performs a direct comparison using the C equality operator. The function is part of PostgreSQL's WAL management system and allows SQL queries to compare LSN values to determine if they reference the same position in the transaction log.

LSN values are crucial for replication, point-in-time recovery, and ensuring data consistency across PostgreSQL instances. This comparison function enables applications and internal PostgreSQL processes to determine if two log positions are identical.

## Parameters / Member Variables
- **Argument 0**: First LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)
- **Argument 1**: Second LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN from function arguments)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from:
  - SQL queries using the = operator on pg_lsn values
  - Internal PostgreSQL code requiring LSN equality checks

## Notes and Other Information
- XLogRecPtr is internally a uint64, making the comparison straightforward and efficient
- This function is automatically invoked when using the equality operator (=) in SQL with pg_lsn operands
- The function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/pg_lsn.c:118-126