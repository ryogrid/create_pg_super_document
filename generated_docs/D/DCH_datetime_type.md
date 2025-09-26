# DCH_datetime_type

## Location
src/backend/utils/adt/formatting.c: 3976 - 4072

## Overview
Analyzes a format node chain to determine which types of date/time components are present, returning a bitmask indicating whether the format contains date, time, or timezone information.

## Definition
```c
static int DCH_datetime_type(FormatNode *node)
```

## Detailed Description
This function traverses a chain of FormatNode structures representing a parsed date/time format string and analyzes the formatting elements to categorize them into three main component types:

- **DCH_DATED**: Date-related components (years, months, days, weekdays, quarters, etc.)
- **DCH_TIMED**: Time-related components (hours, minutes, seconds, milliseconds, microseconds, etc.) 
- **DCH_ZONED**: Timezone-related components (timezone names, offsets, etc.)

The function uses a large switch statement to classify each formatting token by its ID and sets the appropriate flags in the return value. This classification is essential for determining what type of PostgreSQL data type should be used for parsing or formatting operations and helps validate format string compatibility with input data types.

## Parameters / Member Variables
- `node`: Pointer to the first FormatNode in a chain representing parsed format elements. The chain is terminated by a node with type NODE_TYPE_END.

## Dependencies
- Functions called/Symbols referenced:
  - FormatNode (structure type)
  - NODE_TYPE_END (constant)
  - NODE_TYPE_ACTION (constant)
  - Multiple DCH format constants (DCH_YYYY, DCH_MM, DCH_DD, DCH_HH, etc.)
  - DCH_DATED (flag constant)
  - DCH_TIMED (flag constant)
  - DCH_ZONED (flag constant)
- Called from:
  - datetime_format_has_tz
  - do_to_timestamp

## Notes and Other Information
- The function ignores DCH_FX (fill mode) tokens as they don't contribute to component type classification
- Returns a bitmask that can contain multiple flags if the format includes different component types
- Essential for PostgreSQL's type system to validate format string compatibility with timestamp, timestamptz, date, and time data types
- The comprehensive switch statement handles all supported PostgreSQL date/time format tokens including ISO week dates, Julian dates, and various case variations