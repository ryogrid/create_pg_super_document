# DCH_to_char

## Location
src/backend/utils/adt/formatting.c: 2765 - 2847

## Overview
A static function that processes a list of format nodes to convert date/time data from a TmToChar structure into a formatted output string according to PostgreSQL's date/time formatting rules.

## Definition
```c
static void DCH_to_char(FormatNode *node, bool is_interval, TmToChar *in, char *out, Oid collid)
```

## Detailed Description
This function is the core engine for converting date/time data into formatted text strings. It iterates through a linked list of FormatNode structures, processing each formatting directive and outputting the corresponding formatted text. The function handles a comprehensive set of date/time format specifiers including hours (12/24 hour formats), minutes, seconds, AM/PM indicators, and various suffix modifiers (ordinals, fill modes). It supports both regular date/time values and intervals, with special handling for negative values in interval mode.

The function workflow:
1. Caches localized time data (days/months) for performance
2. Iterates through the format node list until NODE_TYPE_END
3. For literal characters, copies them directly to output
4. For action nodes, processes the specific format directive via a large switch statement
5. Handles format modifiers like FM (fill mode) and TH (ordinal suffix)
6. Advances the output pointer after each operation

## Parameters / Member Variables
- `node`: Linked list of FormatNode structures defining the output format
- `is_interval`: Boolean indicating whether formatting an interval (affects hour display)
- `in`: TmToChar structure containing the date/time data to format
- `out`: Output buffer where the formatted string will be written
- `collid`: Collation ID for locale-specific formatting operations

## Dependencies
- Functions called/Symbols referenced:
  - [cache_locale_time](../c/cache_locale_time.md) (for caching localized day/month names)
  - [str_numth](../s/str_numth.md) (for ordinal number formatting)
  - strcpy, sprintf, strlen (standard C library functions)
  - Various DCH format constants (DCH_HH, DCH_AM, etc.)
  - Format suffix macros (S_FM, S_THth, S_TH_TYPE)
  - Time constants (HOURS_PER_DAY, A_M_STR, PM_STR, etc.)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1042)
  - [datetime_to_char_body](../d/datetime_to_char_body.md) (formatting.c:4227)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Contains a very large switch statement handling numerous date/time format codes
- Handles special cases like 12-hour clock display (where hour 0 becomes 12)
- Supports format modifiers including fill mode (FM) and ordinal suffixes (TH/th)
- Distinguishes between different AM/PM case variations (AM/PM, A.M./P.M., am/pm, a.m./p.m.)
- Used extensively in PostgreSQL's TO_CHAR() function for date/time formatting
- Function assumes output buffer is sufficiently large for the formatted result
- Processes format nodes sequentially, building the output string incrementally
- The function is quite extensive, handling many format specifiers beyond the basic ones shown in the truncated source