# timestamptz_in

## Location
[src/backend/utils/adt/timestamp.c:416-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L416-L488)

## Overview
Converts a string representation to internal form of a timestamp with time zone (timestamptz) type in PostgreSQL.

## Definition
```c
Datum timestamptz_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamptz_in` function is the input function for the timestamptz data type in PostgreSQL. It parses a string representation of a timestamp with time zone and converts it to the internal timestamp representation. The function handles various timestamp formats including regular dates, epoch timestamps, and special values like "infinity" and "-infinity". It performs comprehensive parsing through `ParseDateTime` and `DecodeDateTime`, handles timezone information, and applies type modifier adjustments for precision control.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `str` (char *): Input string to be parsed into timestamptz
  - `typelem` (Oid): Type element OID (not used, marked with NOT_USED)
  - `typmod` (int32): Type modifier for precision specification
  - `escontext` (Node *): Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - [ParseDateTime](../P/ParseDateTime.md)
  - [DecodeDateTime](../D/DecodeDateTime.md)
  - DateTimeParseError
  - [tm2timestamp](tm2timestamp.md)
  - [SetEpochTimestamp](../S/SetEpochTimestamp.md)
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)
  - PG_RETURN_TIMESTAMPTZ
  - PG_RETURN_NULL
  - ereturn
  - elog
  - TIMESTAMP_NOEND
  - TIMESTAMP_NOBEGIN
- Types referenced:
  - TimestampTz
  - fsec_t
  - [pg_tm](../p/pg_tm.md)
  - DateTimeErrorExtra
- Constants referenced:
  - MAXDATEFIELDS
  - MAXDATELEN
  - DTK_DATE, DTK_EPOCH, DTK_LATE, DTK_EARLY
- Called from (representative examples):
  - [validateRecoveryParameters](../v/validateRecoveryParameters.md)
  - [CreateRole](../C/CreateRole.md)
  - [AlterRole](../A/AlterRole.md)

## Notes and Other Information
- Handles special timestamp values: epoch, infinity (-infinity, +infinity)
- Supports comprehensive datetime string parsing with timezone information
- Uses PostgreSQL's soft error handling mechanism for graceful error reporting
- Applies type modifier precision adjustments after successful parsing
- Located in src/backend/utils/adt/timestamp.c:416-488
- The function processes various datetime formats and extracts timezone information
- Error handling includes detailed error context through DateTimeErrorExtra structure
- Timezone conversion is handled through the tz parameter in tm2timestamp