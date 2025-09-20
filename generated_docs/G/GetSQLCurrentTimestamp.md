# GetSQLCurrentTimestamp

## Location
[src/backend/utils/adt/timestamp.c:1672-1685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1672-L1685)

## Overview
Implements the SQL CURRENT_TIMESTAMP and CURRENT_TIMESTAMP(n) functions by returning the current transaction's start timestamp with optional precision adjustment.

## Definition

```c
struct timeval tp;
```
## Detailed Description
This function provides the implementation for PostgreSQL's CURRENT_TIMESTAMP SQL function. It returns the timestamp when the current transaction was started, ensuring that all calls to CURRENT_TIMESTAMP within a single transaction return the same value for consistency. The function can optionally adjust the precision of the returned timestamp based on the typmod parameter.

The function maintains SQL standard compliance by using the transaction start time rather than the actual current time, which ensures that multiple references to CURRENT_TIMESTAMP within the same transaction yield identical results.

## Parameters / Member Variables
- : The type modifier specifying the desired precision for the timestamp. If >= 0, the timestamp precision will be adjusted to this value. If < 0, no precision adjustment is performed.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionStartTimestamp](GetCurrentTransactionStartTimestamp.md)
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md)
  - TimestampTzPlusSeconds

## Notes and Other Information
- The function is located in src/backend/utils/adt/timestamp.c:1672-1685
- Returns TimestampTz (timestamp with timezone) type
- Ensures transaction-level consistency for CURRENT_TIMESTAMP calls
- The typmod parameter follows PostgreSQL's standard type modifier conventions for timestamp precision
- Part of PostgreSQL's SQL standard timestamp function implementation