# timetz_time

## Location
[src/backend/utils/adt/date.c:2815-2827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2815-L2827)

## Overview
Extracts the time portion from a time with time zone (TimeTzADT) value, discarding the timezone information and returning a plain time (TimeADT) value.

## Definition


## Detailed Description
This function performs a conversion from a time with time zone data type to a plain time data type. It takes a TimeTzADT (time with timezone) input parameter and extracts only the time component, effectively "swallowing" or ignoring the timezone information. The function is straightforward - it simply accesses the time field of the TimeTzADT structure and returns it as a TimeADT value. This conversion is useful when timezone information is not needed and only the time portion is required for operations or display.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_TIMETZADT_P(0)): A TimeTzADT pointer representing the time with timezone value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Macro to extract TimeTzADT argument from function call
  - PG_RETURN_TIMEADT: Macro to return TimeADT result
- Types used:
  - TimeTzADT: Time with timezone data type
  - TimeADT: Plain time data type
- Called from (representative examples):
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md): Used in JSON path execution for datetime method processing

## Notes and Other Information
- This is a simple extraction function that performs no validation or complex processing
- The timezone component is completely discarded in the conversion
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS
- Located in src/backend/utils/adt/date.c, which contains various date/time utility functions
- The conversion is lossless for the time component but lossy for timezone information