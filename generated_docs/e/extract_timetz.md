# extract_timetz

## Location
[src/backend/utils/adt/date.c:3050-3059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L3050-L3059)

## Overview
Extracts specified field from a time with time zone (TIMETZ) data type, returning the result as a numeric value with precise decimal representation.

## Definition

```c
Datum
extract_timetz(PG_FUNCTION_ARGS)
```
## Detailed Description
`extract_timetz` is a PostgreSQL built-in function that extracts a specific field from a time with time zone value, similar to `timetz_part` but with a key difference in return type precision. It serves as a wrapper function that calls `timetz_part_common` with `retnumeric=true`, ensuring the result is returned as a PostgreSQL numeric type rather than a floating-point number. This provides exact decimal precision for extracted values, which is particularly important for fractional seconds and financial-grade applications.

The function is typically used internally by PostgreSQL when precise numeric results are required from EXTRACT operations or when the system needs to avoid floating-point precision issues.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `units` (text): The field name to extract (e.g., 'hour', 'minute', 'second', 'timezone')
  - `time` (TimeTzADT*): The input time with time zone value

## Dependencies
- Functions called/Symbols referenced:
  - [timetz_part_common](../t/timetz_part_common.md)
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMETZADT_P
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md)
  - [DecodeUnits](../D/DecodeUnits.md)
  - [DecodeSpecial](../D/DecodeSpecial.md)
  - [timetz2tm](../t/timetz2tm.md)
  - PG_RETURN_NUMERIC
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md)
  - [int64_to_numeric](../i/int64_to_numeric.md)
  - ereport
- Called from (representative examples):
  - Internal PostgreSQL numeric extraction operations
  - System functions requiring precise decimal results

## Notes and Other Information
- Primary difference from `timetz_part` is the return of numeric type instead of float8, providing exact decimal precision
- Supports the same field types as `timetz_part`: hour, minute, second, microsecond, millisecond, timezone components
- Fractional seconds (millisecond, second) use specialized numeric division functions to maintain precision
- Epoch extraction returns precise numeric representation of seconds since Unix epoch
- Error handling is identical to `timetz_part` for unsupported or invalid field names
- This function is crucial for applications requiring exact decimal arithmetic without floating-point precision loss