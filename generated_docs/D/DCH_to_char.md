# DCH_to_char

## Location
[src/backend/utils/adt/formatting.c:2765-2847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2765-L2847)

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

## Simplified Source

```c
static void DCH_to_char(FormatNode *node, bool is_interval, TmToChar *in, char *out, Oid collid) {
    FormatNode *n;
    char *s;
    struct fmt_tm *tm = &in->tm;

    // Cache localized time data for performance
    cache_locale_time();

    s = out;

    // Process each format node until end marker
    for (n = node; n->type != NODE_TYPE_END; n++) {

        // Copy literal characters directly
        if (n->type != NODE_TYPE_ACTION) {
            strcpy(s, n->character);
            s += strlen(s);
            continue;
        }

        // Process format codes
        switch (n->key->id) {
            // AM/PM indicators (various case formats)
            case DCH_A_M:
            case DCH_P_M:
                strcpy(s, (tm->tm_hour >= 12) ? P_M_STR : A_M_STR);
                break;
            case DCH_AM:
            case DCH_PM:
                strcpy(s, (tm->tm_hour >= 12) ? PM_STR : AM_STR);
                break;
            case DCH_a_m:
            case DCH_p_m:
                strcpy(s, (tm->tm_hour >= 12) ? p_m_STR : a_m_STR);
                break;
            case DCH_am:
            case DCH_pm:
                strcpy(s, (tm->tm_hour >= 12) ? pm_STR : am_STR);
                break;

            // 12-hour format
            case DCH_HH:
            case DCH_HH12:
                int hour12 = tm->tm_hour % 12;
                if (hour12 == 0) hour12 = 12;
                sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2, hour12);
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                break;

            // 24-hour format
            case DCH_HH24:
                sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_hour);
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                break;

            // Minutes
            case DCH_MI:
                sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_min);
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                break;

            // Seconds
            case DCH_SS:
                sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_sec);
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                break;

            // ... many more format codes handled in full implementation
            default:
                // Handle other format codes (abbreviated for clarity)
                break;
        }

        s += strlen(s);
    }
}
```