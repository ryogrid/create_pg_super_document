# pg_xact_commit_timestamp

## Location
[src/backend/access/transam/commit_ts.c:397-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L397-L419)

## Overview
A PostgreSQL SQL function that returns the commit timestamp for a given transaction ID.

## Definition
```c
Datum pg_xact_commit_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-callable interface to retrieve the commit timestamp of a specific transaction. It serves as a wrapper around the internal TransactionIdGetCommitTsData function, making commit timestamp information accessible from SQL queries. The function returns NULL if no commit timestamp data is available for the specified transaction ID, which can occur if the transaction never committed, was rolled back, or if commit timestamp tracking was not enabled when the transaction occurred.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `xid`: Transaction ID (TransactionId) extracted via PG_GETARG_TRANSACTIONID(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID
  - [TransactionIdGetCommitTsData](../T/TransactionIdGetCommitTsData.md)
  - PG_RETURN_NULL
  - PG_RETURN_TIMESTAMPTZ
- Called from (representative examples):
  - SQL queries via PostgreSQL's function call mechanism

## Notes and Other Information
- Exposed as a SQL function that can be called from SQL queries
- Returns NULL when no commit timestamp data is available for the transaction
- Uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS)
- Located in src/backend/access/transam/commit_ts.c:397-419  
- Part of PostgreSQL's system for tracking transaction commit timestamps
- Requires track_commit_timestamp to be enabled to return meaningful data for transactions
- The returned timestamp represents when the transaction was committed, not when it started

## Simplified Source

```c
Datum pg_xact_commit_timestamp(PG_FUNCTION_ARGS)
{
    TransactionId xid = PG_GETARG_TRANSACTIONID(0);
    TimestampTz ts;
    bool found;

    // Get commit timestamp data for the transaction
    found = TransactionIdGetCommitTsData(xid, &ts, NULL);

    // Return NULL if no commit timestamp found
    if (!found)
        PG_RETURN_NULL();

    PG_RETURN_TIMESTAMPTZ(ts);
}
```