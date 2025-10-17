# now

## Location
[src/backend/utils/adt/timestamp.c:1618-1623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1618-L1623)

## Overview
Returns the timestamp with timezone representing the start time of the current transaction, implementing PostgreSQL's NOW() SQL function.

## Definition

```c
Datum
now(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's NOW() built-in SQL function, which returns the current timestamp with timezone. Importantly, it returns the timestamp representing when the current transaction began, not the exact moment the function is called. This ensures that all calls to NOW() within the same transaction return the same value, providing transaction-level consistency.

The function is a simple wrapper around PostgreSQL's transaction management system, delegating to  to retrieve the cached transaction start time. This design ensures that temporal queries within a transaction have a consistent reference point, which is crucial for maintaining data consistency and supporting features like transaction isolation.

The returned value includes timezone information, making it suitable for applications that need timezone-aware timestamp operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionStartTimestamp](../G/GetCurrentTransactionStartTimestamp.md) (retrieves transaction start timestamp)
  - PG_RETURN_TIMESTAMPTZ (macro to return timestamptz value to PostgreSQL)
- Called from:
  - SQL queries using NOW() function

## Notes and Other Information
- Available as SQL function: NOW()
- Returns transaction start time, not current system time (for current system time, use clock_timestamp())
- Provides transaction-level consistency - all NOW() calls in same transaction return identical value
- Returns TIMESTAMPTZ (timestamp with timezone) data type
- Part of PostgreSQL's temporal function suite alongside CURRENT_TIMESTAMP
- Function follows PostgreSQL's PG_FUNCTION_ARGS calling convention for SQL-callable functions
- Critical for maintaining temporal consistency in database transactions

## Simplified Source

```c
Datum now(PG_FUNCTION_ARGS) {
    // Return transaction start timestamp with timezone
    return PG_RETURN_TIMESTAMPTZ(GetCurrentTransactionStartTimestamp());
}
```