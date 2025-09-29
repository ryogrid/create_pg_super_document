# AdjustTimestampForTypmod

## Location
[src/backend/utils/adt/timestamp.c:366-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L366-L415)

## Overview
Rounds off a timestamp value to match a specified type modifier precision, working for both timestamp and timestamptz types.

## Definition
```c
bool AdjustTimestampForTypmod(Timestamp *time, int32 typmod, Node *escontext)
```

## Detailed Description
The `AdjustTimestampForTypmod` function adjusts timestamp precision by rounding the timestamp value according to the specified type modifier. It uses precomputed scale and offset arrays to efficiently perform the precision adjustment. The function handles both positive and negative timestamps correctly, applying appropriate rounding logic. It supports precision levels from 0 to MAX_TIMESTAMP_PRECISION (6) microseconds, with proper error handling for invalid precision values.

## Parameters / Member Variables
- `time` (Timestamp *): Pointer to the timestamp value to be adjusted (modified in-place)
- `typmod` (int32): Type modifier specifying the desired precision (0-6, or -1 for no adjustment)
- `escontext` (Node *): Error context for soft error handling (can be NULL for hard errors)

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - MAX_TIMESTAMP_PRECISION
  - INT64CONST
  - ereturn
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - ERRCODE_INVALID_PARAMETER_VALUE
- Called from (representative examples):
  - [timestamp_in](../t/timestamp_in.md)
  - [timestamp_recv](../t/timestamp_recv.md)
  - [timestamp_scale](../t/timestamp_scale.md)
  - [timestamptz_in](../t/timestamptz_in.md)
  - [timestamptz_recv](../t/timestamptz_recv.md)
  - [timestamptz_scale](../t/timestamptz_scale.md)
  - [to_timestamp](../t/to_timestamp.md)
  - [parse_datetime](../p/parse_datetime.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - [GetSQLCurrentTimestamp](../G/GetSQLCurrentTimestamp.md)
  - [GetSQLLocalTimestamp](../G/GetSQLLocalTimestamp.md)

## Notes and Other Information
- Uses static lookup tables (TimestampScales and TimestampOffsets) for efficient precision adjustment
- Supports PostgreSQL's soft error handling mechanism via escontext parameter
- Returns true on success, false on failure when using soft error handling
- Handles special cases like infinite timestamps (TIMESTAMP_NOT_FINITE)
- The rounding algorithm adds half the scale unit before truncating to achieve proper rounding
- Located in src/backend/utils/adt/timestamp.c:366-415
- Precision 0 = seconds, 1 = deciseconds, ..., 6 = microseconds

## Simplified Source

```c
bool
AdjustTimestampForTypmod(Timestamp *time, int32 typmod, Node *escontext)
{
    // Lookup tables for precision adjustment
    static const int64 TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
        1000000, 100000, 10000, 1000, 100, 10, 1
    };
    static const int64 TimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
        500000, 50000, 5000, 500, 50, 5, 0
    };

    // Skip adjustment for infinite timestamps or default precision
    if (TIMESTAMP_NOT_FINITE(*time) || typmod == -1 || typmod == MAX_TIMESTAMP_PRECISION) {
        return true;
    }

    // Validate typmod range
    if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION) {
        ereturn(escontext, false,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("timestamp(%d) precision must be between %d and %d",
                        typmod, 0, MAX_TIMESTAMP_PRECISION)));
    }

    // Round timestamp to specified precision
    if (*time >= 0) {
        *time = ((*time + TimestampOffsets[typmod]) / TimestampScales[typmod]) *
                TimestampScales[typmod];
    } else {
        *time = -((((-*time) + TimestampOffsets[typmod]) / TimestampScales[typmod]) *
                  TimestampScales[typmod]);
    }

    return true;
}
```