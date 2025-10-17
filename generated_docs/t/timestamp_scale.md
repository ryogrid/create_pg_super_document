# timestamp_scale

## Location
[src/backend/utils/adt/timestamp.c:345-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L345-L365)

## Overview
Adjusts a timestamp value for a specified scale factor as used by the PostgreSQL type system to handle column type modifications.

## Definition
```c
Datum timestamp_scale(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_scale` function is a PostgreSQL built-in function that adjusts timestamp values according to a specified type modifier (typmod). This function is primarily used by the PostgreSQL type system when handling columns with specific precision requirements. It takes a timestamp value and a typmod parameter, then delegates the actual adjustment work to `AdjustTimestampForTypmod` to ensure the timestamp conforms to the required precision.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `timestamp` (Timestamp): The input timestamp value to be adjusted
  - `typmod` (int32): The type modifier specifying the desired precision/scale

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_INT32
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)
  - PG_RETURN_TIMESTAMP
  - Timestamp (type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function serves as a wrapper around `AdjustTimestampForTypmod`, providing the PostgreSQL function interface
- Located in src/backend/utils/adt/timestamp.c:345-365
- Used internally by PostgreSQL type system for column type adjustments
- The function passes NULL as the third argument to `AdjustTimestampForTypmod`, indicating no special error handling context

## Simplified Source

```c
Datum timestamp_scale(PG_FUNCTION_ARGS) {
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    int32 typmod = PG_GETARG_INT32(1);
    Timestamp result;

    // Copy input timestamp
    result = timestamp;

    // Adjust precision according to typmod
    AdjustTimestampForTypmod(&result, typmod, NULL);

    PG_RETURN_TIMESTAMP(result);
}
```