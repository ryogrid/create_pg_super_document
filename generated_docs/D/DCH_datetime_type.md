# DCH_datetime_type

## Location
[src/backend/utils/adt/formatting.c:3976-4072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L3976-L4072)

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
  - [FormatNode](../F/FormatNode.md) (structure type)
  - NODE_TYPE_END (constant)
  - NODE_TYPE_ACTION (constant)
  - Multiple DCH format constants (DCH_YYYY, DCH_MM, DCH_DD, DCH_HH, etc.)
  - DCH_DATED (flag constant)
  - DCH_TIMED (flag constant)
  - DCH_ZONED (flag constant)
- Called from:
  - [datetime_format_has_tz](../d/datetime_format_has_tz.md)
  - [do_to_timestamp](../d/do_to_timestamp.md)

## Notes and Other Information
- The function ignores DCH_FX (fill mode) tokens as they don't contribute to component type classification
- Returns a bitmask that can contain multiple flags if the format includes different component types
- Essential for PostgreSQL's type system to validate format string compatibility with timestamp, timestamptz, date, and time data types
- The comprehensive switch statement handles all supported PostgreSQL date/time format tokens including ISO week dates, Julian dates, and various case variations

## Simplified Source

```c
static int
DCH_datetime_type(FormatNode *node)
{
    FormatNode *n;
    int flags = 0;

    // Scan through format nodes to determine component types
    for (n = node; n->type != NODE_TYPE_END; n++) {
        if (n->type != NODE_TYPE_ACTION)
            continue;

        switch (n->key->id) {
            case DCH_FX:
                // Fill mode - doesn't affect component type
                break;

            // Time components
            case DCH_A_M: case DCH_P_M: case DCH_a_m: case DCH_p_m:
            case DCH_AM: case DCH_PM: case DCH_am: case DCH_pm:
            case DCH_HH: case DCH_HH12: case DCH_HH24:
            case DCH_MI: case DCH_SS:
            case DCH_MS: case DCH_US:  // millisecond, microsecond
            case DCH_FF1: case DCH_FF2: case DCH_FF3:
            case DCH_FF4: case DCH_FF5: case DCH_FF6:
            case DCH_SSSS:
                flags |= DCH_TIMED;
                break;

            // Timezone components
            case DCH_tz: case DCH_TZ: case DCH_OF:
            case DCH_TZH: case DCH_TZM:
                flags |= DCH_ZONED;
                break;

            // Date components
            case DCH_A_D: case DCH_B_C: case DCH_a_d: case DCH_b_c:
            case DCH_AD: case DCH_BC: case DCH_ad: case DCH_bc:
            case DCH_MONTH: case DCH_Month: case DCH_month:
            case DCH_MON: case DCH_Mon: case DCH_mon:
            case DCH_MM:
            case DCH_DAY: case DCH_Day: case DCH_day:
            case DCH_DY: case DCH_Dy: case DCH_dy:
            case DCH_DDD: case DCH_IDDD: case DCH_DD: case DCH_D: case DCH_ID:
            case DCH_WW: case DCH_Q: case DCH_CC:
            case DCH_Y_YYY: case DCH_YYYY: case DCH_IYYY:
            case DCH_YYY: case DCH_IYY: case DCH_YY: case DCH_IY:
            case DCH_Y: case DCH_I:
            case DCH_RM: case DCH_rm: case DCH_W: case DCH_J:
                flags |= DCH_DATED;
                break;
        }
    }

    return flags;
}
```