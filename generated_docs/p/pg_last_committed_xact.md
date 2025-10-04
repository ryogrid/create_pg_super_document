# pg_last_committed_xact

## Location
[src/backend/access/transam/commit_ts.c:420-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L420-L463)

## Overview
A PostgreSQL SQL function that returns comprehensive information about the latest committed transaction, including transaction ID, commit timestamp, and replication origin.

## Definition
```c
Datum pg_last_committed_xact(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-callable interface to retrieve detailed information about the most recently committed transaction. It returns a composite row type containing the transaction ID, commit timestamp, and replication origin node ID. The function constructs a tuple with this data and handles cases where no valid transaction data is available by returning NULLs for all fields. This is particularly useful for monitoring, replication management, and debugging purposes.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure (no explicit arguments expected)

## Dependencies
- Functions called/Symbols referenced:
  - [GetLatestCommitTsData](../G/GetLatestCommitTsData.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - TransactionIdIsNormal
  - [TransactionIdGetDatum](../T/TransactionIdGetDatum.md)
  - [TimestampTzGetDatum](../T/TimestampTzGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - SQL queries via PostgreSQL's function call mechanism

## Notes and Other Information
- Returns a composite row type with three columns: transaction ID, timestamp, and origin node ID
- Uses GetLatestCommitTsData internally to retrieve the raw data
- Returns NULLs for all columns if no normal transaction ID is available
- Validates return type must be composite (row type) at runtime
- Located in src/backend/access/transam/commit_ts.c:420-463
- Part of PostgreSQL's system information functions for transaction monitoring
- Requires track_commit_timestamp to be enabled to return meaningful commit timestamp data
- Particularly useful for logical replication and monitoring scenarios where transaction timing information is needed

## Simplified Source

```c
Datum pg_last_committed_xact(PG_FUNCTION_ARGS)
{
    TransactionId xid;
    RepOriginId nodeid;
    TimestampTz ts;
    Datum values[3];
    bool nulls[3];
    TupleDesc tupdesc;
    HeapTuple htup;

    // Get latest commit timestamp data
    xid = GetLatestCommitTsData(&ts, &nodeid);

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    if (!TransactionIdIsNormal(xid))
    {
        // No valid transaction - return all NULLs
        memset(nulls, true, sizeof(nulls));
    }
    else
    {
        // Populate return values
        values[0] = TransactionIdGetDatum(xid);
        nulls[0] = false;

        values[1] = TimestampTzGetDatum(ts);
        nulls[1] = false;

        values[2] = ObjectIdGetDatum((Oid) nodeid);
        nulls[2] = false;
    }

    // Construct and return tuple
    htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```