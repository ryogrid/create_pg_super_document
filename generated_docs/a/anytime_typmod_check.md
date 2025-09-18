# anytime_typmod_check

## Location
src/backend/utils/adt/date.c: 71 - 92

## Overview
Validates and normalizes precision values for TIME data types, ensuring they fall within acceptable bounds and issuing appropriate warnings or errors.

## Definition


## Detailed Description
This function validates the precision (type modifier) value for TIME data types with or without time zone. It performs bounds checking to ensure the precision is not negative and does not exceed the maximum allowed precision. If the precision exceeds the maximum, it issues a warning and clamps the value to the maximum allowed precision. The function is exported so that parse_expr.c can use it for SQL value function transformations.

## Parameters / Member Variables
- : Boolean flag indicating whether this is for a TIME WITH TIME ZONE type (true) or TIME WITHOUT TIME ZONE type (false), used for error message formatting
- : The precision value to validate (number of fractional seconds digits)

## Dependencies
- Functions called/Symbols referenced:
  - MAX_TIME_PRECISION (constant defining maximum allowed time precision)
- Called from (representative examples):
  - [anytime_typmodin](anytime_typmodin.md) (src/backend/utils/adt/date.c:66)
  - transformSQLValueFunction (src/backend/parser/parse_expr.c:2318, 2332)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md) (src/backend/utils/adt/jsonpath_exec.c:2576, 2623)
  - PG_RETURN_TIMETZADT_P (src/include/utils/date.h:99)

## Notes and Other Information
- This function is exported (non-static) to allow usage from other modules, particularly parse_expr.c
- Negative precision values result in an ERROR, terminating the operation
- Precision values exceeding MAX_TIME_PRECISION result in a WARNING but execution continues with the clamped value
- The function returns the validated (and potentially adjusted) precision value
- Error and warning messages include context about whether the type is WITH TIME ZONE or WITHOUT TIME ZONE
- Part of PostgreSQL's type system validation infrastructure for temporal data types