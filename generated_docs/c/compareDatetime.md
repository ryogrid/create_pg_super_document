# compareDatetime

## Location
[src/backend/utils/adt/jsonpath_exec.c:3723-3887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3723-L3887)

## Overview
A static function that performs cross-type comparison of two datetime SQL/JSON items with proper error handling for incompatible types and timezone requirements.

## Definition

```c
static int
compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2,
				bool useTz, bool *cast_error)
```
## Detailed Description
The  function implements comprehensive datetime comparison logic for SQL/JSON path operations. It handles all combinations of PostgreSQL datetime types (DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ) and determines if they can be meaningfully compared. The function employs a nested switch statement structure to handle type-specific conversion and comparison logic. When types are incompatible (such as comparing DATE with TIME), it sets the cast_error flag rather than throwing an exception, allowing callers to handle the error appropriately. For comparable types, it delegates to the appropriate PostgreSQL comparison functions or helper functions for cross-type comparisons.

## Parameters / Member Variables
- : The first datetime value to compare (as a PostgreSQL Datum)
- : The OID of the first value's PostgreSQL type
- : The second datetime value to compare (as a PostgreSQL Datum)  
- : The OID of the second value's PostgreSQL type
- : Boolean flag indicating whether timezone information should be used in comparisons
- : Output parameter set to true if the types are incompatible for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [date_cmp](../d/date_cmp.md)
  - [time_cmp](../t/time_cmp.md)
  - [timetz_cmp](../t/timetz_cmp.md)
  - [timestamp_cmp](../t/timestamp_cmp.md)
  - [cmpDateToTimestamp](cmpDateToTimestamp.md)
  - [cmpDateToTimestampTz](cmpDateToTimestampTz.md)
  - [cmpTimestampToTimestampTz](cmpTimestampToTimestampTz.md)
  - [castTimeToTimeTz](castTimeToTimeTz.md)
  - DatumGetDateADT
  - DatumGetTimestamp
  - DatumGetTimestampTz
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - DirectFunctionCall2
- Called from (representative examples):
  - [compareItems](compareItems.md)
  - RETURN_ERROR

## Notes and Other Information
- Returns 0 when cast_error is set to true (incomparable types)
- Returns negative, zero, or positive integer for less than, equal to, or greater than comparisons respectively
- Throws explicit errors for unrecognized datetime type OIDs
- Handles timezone casting automatically when comparing TIME and TIMETZ types
- Part of the JSON path execution engine in PostgreSQL's JSON functionality