# pg_lsn_smaller

## Location
[src/backend/utils/adt/pg_lsn.c:181-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L181-L190)

## Overview
The pg_lsn_smaller function returns the smaller (earlier) of two PostgreSQL Log Sequence Numbers (LSNs).

## Definition
Datum pg_lsn_smaller(PG_FUNCTION_ARGS)

## Detailed Description
This function implements a minimum operation for the pg_lsn data type, comparing two LSN values and returning the one that represents an earlier position in the PostgreSQL write-ahead log. The function performs a direct comparison of the underlying XLogRecPtr values and returns the smaller one. This is useful in scenarios where you need to determine the earliest LSN from a set of values, such as when finding the oldest checkpoint that needs to be retained or determining the starting point for log replay operations.

Like its counterpart pg_lsn_larger, this function uses a ternary operator for efficient selection and return of the smaller LSN value.

## Parameters / Member Variables
- First argument (index 0): The first LSN value for comparison
- Second argument (index 1): The second LSN value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro to extract LSN argument)
  - PG_RETURN_LSN (macro to return LSN result)
- Called from (representative examples):
  - SQL queries using the pg_lsn_smaller() function
  - WAL management and cleanup operations
  - Replication synchronization logic

## Notes and Other Information
- Returns the LSN value that is numerically smaller (represents an earlier position in the WAL)
- Useful for finding the minimum LSN in log management scenarios
- Part of PostgreSQL's pg_lsn utility functions for WAL position management
- The comparison is based on the natural ordering of LSN values in the write-ahead log sequence
- Commonly used in checkpoint management and log retention policy implementations
- Complementary function to pg_lsn_larger for complete min/max operations on LSNs