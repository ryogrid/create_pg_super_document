# GetSQLLocalTimestamp

## Location
[src/backend/utils/adt/timestamp.c:1686-1699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1686-L1699)

## Overview
Implements the SQL LOCALTIMESTAMP and LOCALTIMESTAMP(n) functions by returning the current transaction's start timestamp converted to local time without timezone information.

## Definition
Timestamp GetSQLLocalTimestamp(int32 typmod)

## Detailed Description
This function provides the implementation for PostgreSQL's LOCALTIMESTAMP SQL function. It returns the timestamp when the current transaction was started, converted to the local timezone and stripped of timezone information. Like GetSQLCurrentTimestamp, it ensures transaction-level consistency by using the transaction start time rather than the actual current time. The key difference is that this function returns a Timestamp (without timezone) rather than TimestampTz.

The function first obtains the transaction start timestamp with timezone, converts it to the local timezone using timestamptz2timestamp(), and optionally adjusts the precision based on the typmod parameter.

## Parameters / Member Variables
- `typmod`: The type modifier specifying the desired precision for the timestamp. If >= 0, the timestamp precision will be adjusted to this value. If < 0, no precision adjustment is performed.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionStartTimestamp](GetCurrentTransactionStartTimestamp.md)
  - [timestamptz2timestamp](../t/timestamptz2timestamp.md)
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md)
  - TimestampTzPlusSeconds

## Notes and Other Information
- The function is located in src/backend/utils/adt/timestamp.c:1686-1699
- Returns Timestamp (timestamp without timezone) type, unlike GetSQLCurrentTimestamp which returns TimestampTz
- Ensures transaction-level consistency for LOCALTIMESTAMP calls
- Uses timestamptz2timestamp() to convert from timestamptz to local timestamp
- Part of PostgreSQL's SQL standard timestamp function implementation
- The conversion to local time respects the current session's timezone setting

## Simplified Source

```c
Timestamp GetSQLLocalTimestamp(int32 typmod) {
    Timestamp ts;

    // Get transaction start timestamp and convert to local time
    ts = timestamptz2timestamp(GetCurrentTransactionStartTimestamp());

    // Apply precision adjustment if specified
    if (typmod >= 0)
        AdjustTimestampForTypmod(&ts, typmod, NULL);

    return ts;
}
```