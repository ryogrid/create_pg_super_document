# pg_xact_commit_timestamp_origin

## Location
[src/backend/access/transam/commit_ts.c:464-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L464-L505)

## Overview
A PostgreSQL SQL function that returns both the commit timestamp and replication origin information for a specific transaction ID.

## Definition
```c
Datum pg_xact_commit_timestamp_origin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-callable interface to retrieve both the commit timestamp and replication origin node ID for a given transaction. It returns a composite row type containing two columns: the commit timestamp and the origin node ID. Unlike pg_xact_commit_timestamp which only returns the timestamp, this function provides the complete commit metadata including replication origin information, making it particularly useful for logical replication scenarios where origin tracking is important.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `xid`: Transaction ID (TransactionId) extracted via PG_GETARG_TRANSACTIONID(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID
  - [TransactionIdGetCommitTsData](../T/TransactionIdGetCommitTsData.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - TimestampTzGetDatum
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL queries via PostgreSQL's function call mechanism

## Notes and Other Information
- Returns a composite row type with two columns: commit timestamp and replication origin node ID
- Returns NULLs for both columns if no commit timestamp data is found for the transaction
- Validates that the return type must be composite (row type) at runtime
- Located in src/backend/access/transam/commit_ts.c:464-505
- Part of PostgreSQL's system for tracking transaction commit timestamps and replication origins
- Requires track_commit_timestamp to be enabled to return meaningful data
- Particularly useful in logical replication environments where tracking the origin of transactions is important
- The origin node ID helps identify which node in a replication cluster originally committed the transaction