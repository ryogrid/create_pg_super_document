# checkTimezoneIsUsedForCast

## Location
[src/backend/utils/adt/jsonpath_exec.c:3666-3677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3666-L3677)

## Overview
A static helper function that validates timezone usage requirements for type casting operations in JSON path execution.

## Definition
```c
static void checkTimezoneIsUsedForCast(bool useTz, const char *type1, const char *type2)
```

## Detailed Description
This function enforces timezone requirements when converting between temporal data types during JSON path execution. It serves as a validation gate that ensures timezone-aware conversions are properly flagged when required. The function will raise an error if a timezone-sensitive cast is attempted without the timezone usage flag being set, preventing potential data corruption or incorrect temporal calculations.

## Parameters / Member Variables
- `useTz`: Boolean flag indicating whether timezone usage is enabled for the cast operation
- `type1`: Source data type name (as string) for error reporting  
- `type2`: Target data type name (as string) for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [errhint](../e/errhint.md) (error hint message)
- Called from (representative examples):
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md) (multiple locations)
  - [castTimeToTimeTz](castTimeToTimeTz.md)
  - [cmpDateToTimestampTz](cmpDateToTimestampTz.md)
  - [cmpTimestampToTimestampTz](cmpTimestampToTimestampTz.md)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonpath_exec.c:3666-3677
- Part of PostgreSQL's JSON path execution engine
- Provides user-friendly error messages suggesting the use of *_tz() functions
- Critical for maintaining temporal data integrity in timezone-sensitive operations
- Uses ERRCODE_FEATURE_NOT_SUPPORTED to indicate unsupported operations without timezone context