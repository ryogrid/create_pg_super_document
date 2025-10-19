# pg_lsn_lt

## Location
[src/backend/utils/adt/pg_lsn.c:136-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L136-L144)

## Overview
Compares two PostgreSQL Log Sequence Number (LSN) values to determine if the first is less than the second, enabling chronological ordering of WAL positions.

## Definition

```c
Datum
pg_lsn_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the less-than operator (<) for the pg_lsn data type in PostgreSQL. It extracts two XLogRecPtr values from the function arguments and performs a direct numerical comparison to determine if the first LSN represents an earlier position in the WAL than the second LSN.

This comparison is fundamental for establishing the chronological order of WAL entries, which is critical for replication lag monitoring, determining recovery points, and implementing time-based queries on LSN values. Since LSNs are monotonically increasing, a smaller LSN value always represents an earlier point in the transaction log timeline.

## Parameters / Member Variables
- **Argument 0**: First LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)
- **Argument 1**: Second LSN value to compare (extracted as XLogRecPtr via PG_GETARG_LSN)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN from function arguments)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from:
  - SQL queries using the < operator on pg_lsn values
  - Internal PostgreSQL code for LSN ordering and chronological comparisons

## Notes and Other Information
- XLogRecPtr is internally a uint64, making the numerical comparison straightforward and efficient
- This function is automatically invoked when using the less-than operator (<) in SQL with pg_lsn operands
- Essential for replication monitoring and WAL progress tracking
- The function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Located in src/backend/utils/adt/pg_lsn.c:136-144

## Simplified Source

```c
Datum pg_lsn_lt(PG_FUNCTION_ARGS) {
    // Extract both LSN values from function arguments
    XLogRecPtr lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr lsn2 = PG_GETARG_LSN(1);

    // Return true if first LSN is less than second (earlier in WAL)
    PG_RETURN_BOOL(lsn1 < lsn2);
}
```