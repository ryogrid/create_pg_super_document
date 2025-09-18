# NonFiniteTimestampTzPart

## Location
src/backend/utils/adt/timestamp.c: 5296 - 5352

## Overview
Handles extraction of date/time parts from infinite timestamp values, returning appropriate infinity values or zero for different unit types.

## Definition
```c
static float8 NonFiniteTimestampTzPart(int type, int unit, char *lowunits, bool isNegative, bool isTz)
```

## Detailed Description
The `NonFiniteTimestampTzPart` function is a specialized handler for extracting date/time parts from infinite timestamp values (both positive and negative infinity). It categorizes time units into two types:

1. **Oscillating units** (return 0.0): These are cyclical components like seconds, minutes, hours, days, months, etc. For infinite timestamps, these don't have meaningful values and return 0.0 (interpreted as NULL by callers).

2. **Monotonically-increasing units** (return ±infinity): These are cumulative measures like years, decades, centuries, Julian days, epochs, etc. For infinite timestamps, these return positive or negative infinity based on the sign of the input timestamp.

The function ensures error handling consistency with finite timestamp extraction functions by throwing identical errors for invalid or unsupported units.

## Parameters / Member Variables
- `type`: The unit type classification (UNITS or RESERV)
- `unit`: The specific time unit constant (DTK_YEAR, DTK_MONTH, etc.)
- `lowunits`: String representation of the unit for error messages
- `isNegative`: Whether the infinite timestamp is negative infinity
- `isTz`: Whether this is for timestamptz (true) or timestamp (false)

## Dependencies
- Functions called/Symbols referenced:
  - get_float8_infinity (returns positive or negative infinity values)
  - Various DTK_* constants (time unit definitions)
  - UNITS, RESERV (unit type constants)
  - ereport, errcode, errmsg (error reporting functions)
  - format_type_be (type formatting for error messages)
- Called from (representative examples):
  - timestamp_part_common
  - timestamptz_part_common

## Notes and Other Information
- Static function used internally by timestamp part extraction functions
- Provides consistent behavior between finite and infinite timestamp handling
- Returns 0.0 for oscillating units, which calling functions interpret as NULL
- Returns appropriate signed infinity for monotonically-increasing units
- Essential for maintaining mathematical consistency in PostgreSQL's infinite timestamp support
- Error messages match those from finite timestamp extraction to ensure uniform user experience