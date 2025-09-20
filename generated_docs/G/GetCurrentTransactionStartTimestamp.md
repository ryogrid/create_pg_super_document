# GetCurrentTransactionStartTimestamp

## Location
[src/backend/access/transam/xact.c:867-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L867-L875)

## Overview
Returns the timestamp when the current transaction was started.

## Definition

```c
TimestampTz
GetCurrentTransactionStartTimestamp(void)
```
## Detailed Description
GetCurrentTransactionStartTimestamp is a simple accessor function that returns the transaction start timestamp stored in the global variable xactStartTimestamp. This timestamp represents the exact moment when the current transaction began and is crucial for various PostgreSQL operations including MVCC (Multi-Version Concurrency Control), time-based SQL functions, and maintaining temporal consistency within transactions. The returned timestamp remains constant throughout the entire transaction, ensuring that all time-related operations within a single transaction see a consistent view of "now".

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - xactStartTimestamp (global variable)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (src/backend/access/transam/parallel.c:351)
  - [timetz_zone](../t/timetz_zone.md) (src/backend/utils/adt/date.c:3086,3094)
  - GetCurrentTimeUsec (src/backend/utils/adt/datetime.c:389)
  - [pg_timezone_abbrevs](../p/pg_timezone_abbrevs.md) (src/backend/utils/adt/datetime.c:5075)
  - [pg_timezone_names](../p/pg_timezone_names.md) (src/backend/utils/adt/datetime.c:5149)
  - [now](../n/now.md) (src/backend/utils/adt/timestamp.c:1620)
  - [GetSQLCurrentTimestamp](GetSQLCurrentTimestamp.md) (src/backend/utils/adt/timestamp.c:1676)
  - [GetSQLLocalTimestamp](GetSQLLocalTimestamp.md) (src/backend/utils/adt/timestamp.c:1690)

## Notes and Other Information
- The timestamp is set once at transaction start and remains constant throughout the transaction
- Used by SQL functions like NOW(), CURRENT_TIMESTAMP, and related time functions
- Essential for maintaining consistent temporal semantics within transactions
- Located in src/backend/access/transam/xact.c:867-875
- Simple getter function with no side effects or error conditions
- Critical component of PostgreSQL's transaction timestamp management system