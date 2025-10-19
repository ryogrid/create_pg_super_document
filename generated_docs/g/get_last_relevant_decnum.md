# get_last_relevant_decnum

## Location
[src/backend/utils/adt/formatting.c:5369-5396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5369-L5396)

## Overview
Finds the position of the last non-zero digit after the decimal point in a numeric string, used for formatting control in PostgreSQL's FM (Fill Mode) formatting.

## Definition

```c
static char *
get_last_relevant_decnum(char *num)
```
## Detailed Description
This function scans a numeric string to find the last significant (non-zero) digit after the decimal point. It's primarily used in Fill Mode (FM) formatting to determine where to truncate trailing zeros. The function returns a pointer to the last relevant character, which could be either the last non-zero digit or the decimal point itself if all decimal digits are zero.

Examples of behavior:
- "12.0500" → returns pointer to '5'
- "12.0000" → returns pointer to '.' (decimal point)
- "12" (no decimal) → returns NULL

## Parameters / Member Variables
- `*num`: Input numeric string to analyze for the last relevant decimal digit
## Dependencies
- Functions called/Symbols referenced:
  - strchr (locates decimal point character)
  - elog (debug logging when DEBUG_TO_FROM_CHAR is enabled)
  - DEBUG_elog_output (debug level constant)
- Called from (representative examples):
  - [NUM_processor](../N/NUM_processor.md) (formatting.c:5925)
  - DCH_ZONED (formatting.c:1078)

## Notes and Other Information
- Returns NULL if no decimal point exists in the input string
- Used specifically for FM (Fill Mode) formatting behavior in to_char() functions
- The function preserves the decimal point position when all decimal digits are zeros
- Part of PostgreSQL's number formatting system that handles precise decimal formatting
- Includes debug logging support when DEBUG_TO_FROM_CHAR compilation flag is enabled
- Simple but critical utility for proper numeric string formatting and trailing zero handling

## Simplified Source

```c
static char *
get_last_relevant_decnum(char *num)
{
    // Find decimal point in the string
    char *p = strchr(num, '.');

    // Return NULL if no decimal point exists
    if (!p)
        return NULL;

    // Start result at decimal point position
    char *result = p;

    // Scan forward from decimal point to find last non-zero digit
    while (*(++p))
    {
        if (*p != '0')
            result = p;  // Update to latest non-zero position
    }

    // Return pointer to last relevant character
    // (either last non-zero digit or decimal point if all zeros)
    return result;
}
```