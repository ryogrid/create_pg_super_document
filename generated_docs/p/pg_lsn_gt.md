# pg_lsn_gt

## Location
[src/backend/utils/adt/pg_lsn.c:145-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L145-L153)

## Overview
Compares two PostgreSQL Log Sequence Number (LSN) values to determine if the first is greater than the second, enabling identification of later WAL positions.

## Definition

```c
Datum
pg_lsn_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the greater-than operator (>) for the pg_lsn data type in PostgreSQL. It extracts two XLogRecPtr values from the function arguments and performs a direct numerical comparison to determine if the first LSN represents a later position in the WAL than the second LSN.

This comparison is essential for identifying more recent WAL positions, monitoring replication progress, and implementing queries that need to find LSN values that occurred after a specific point in time. The function is commonly used in replication monitoring to determine if a replica has advanced beyond a certain LSN threshold.

## Parameters / Member Variables
- **Argument 0**: First LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)
- **Argument 1**: Second LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN from function arguments)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from:
  - SQL queries using the > operator on pg_lsn values
  - Internal PostgreSQL code for LSN ordering and chronological comparisons

## Notes and Other Information
- XLogRecPtr is internally a uint64, making the numerical comparison straightforward and efficient
- This function is automatically invoked when using the greater-than operator (>) in SQL with pg_lsn operands
- Useful for determining if replication has progressed beyond a specific point
- The function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/pg_lsn.c:145-153

## Simplified Source

```c
Datum pg_lsn_gt(PG_FUNCTION_ARGS) {
    // Extract both LSN values from function arguments
    XLogRecPtr lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr lsn2 = PG_GETARG_LSN(1);

    // Return true if first LSN is greater than second (later in WAL)
    PG_RETURN_BOOL(lsn1 > lsn2);
}
```