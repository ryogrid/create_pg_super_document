# pg_wal_lsn_diff

## Location
[src/backend/access/transam/xlogfuncs.c:651-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L651-L668)

## Overview
Computes the difference in bytes between two WAL (Write-Ahead Log) LSN (Log Sequence Number) positions.

## Definition
```c
Datum pg_wal_lsn_diff(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the byte difference between two WAL LSN positions. LSNs represent specific locations in the PostgreSQL Write-Ahead Log, and the difference between them indicates how many bytes of WAL data exist between those two positions.

The function takes two LSN values as arguments and returns the numeric difference in bytes. This is particularly useful for:
- Measuring WAL generation rate over time
- Calculating replication lag in bytes
- Determining storage requirements for WAL archiving
- Performance monitoring and capacity planning

Internally, the function delegates the actual computation to the pg_lsn_mi function using DirectFunctionCall2, which performs the LSN subtraction operation.

## Parameters / Member Variables
- Takes two parameters via PG_FUNCTION_ARGS:

## Dependencies
- Functions called/Symbols referenced:
  - [pg_lsn_mi](pg_lsn_mi.md) (performs the actual LSN subtraction)
  - DirectFunctionCall2 (PostgreSQL function call mechanism)
  - PG_GETARG_DATUM (macro to retrieve function arguments)
  - PG_RETURN_DATUM (macro to return result)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Returns the difference as a numeric value representing bytes
- The result can be negative if the first LSN is smaller than the second LSN
- Commonly used in monitoring queries to track WAL activity and replication status
- Part of PostgreSQL's WAL management and monitoring infrastructure
- The function is a thin wrapper around the core pg_lsn_mi functionality
- Defined in src/backend/access/transam/xlogfuncs.c:651-668