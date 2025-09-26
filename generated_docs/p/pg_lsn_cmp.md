# pg_lsn_cmp

## Location
[src/backend/utils/adt/pg_lsn.c:191-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L191-L205)

## Overview
The pg_lsn_cmp function provides a three-way comparison of PostgreSQL Log Sequence Numbers (LSNs) for btree index support, returning -1, 0, or 1 based on the relative ordering of two LSN values.

## Definition
Datum pg_lsn_cmp(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the comparison operator required for btree index support on the pg_lsn data type. It follows the standard three-way comparison convention used throughout PostgreSQL's indexing system. The function compares two XLogRecPtr values and returns:
- 1 if the first LSN is greater than the second (later in the WAL sequence)
- 0 if the LSNs are equal (same position in the WAL)
- -1 if the first LSN is less than the second (earlier in the WAL sequence)

This function is essential for creating btree indexes on pg_lsn columns, enabling efficient ordering and range queries on LSN values. The three-way comparison result allows the btree implementation to properly organize LSN values in the index structure.

## Parameters / Member Variables
- First argument (index 0): The left operand LSN value (a) for comparison
- Second argument (index 1): The right operand LSN value (b) for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro to extract LSN argument)
  - PG_RETURN_INT32 (macro to return integer result)
- Called from (representative examples):
  - PostgreSQL's btree index operations when indexing pg_lsn columns
  - [Sort](../S/Sort.md) operations on pg_lsn values
  - ORDER BY clauses involving pg_lsn columns

## Notes and Other Information
- This is the canonical comparison function for pg_lsn btree index support
- The function explicitly handles all three comparison cases with clear conditional logic
- Essential for creating indexes on pg_lsn columns in PostgreSQL tables
- Enables efficient sorting and range queries on LSN values
- Part of the pg_lsn opclass definition for btree indexes
- The comparison is deterministic and consistent with the natural ordering of WAL positions