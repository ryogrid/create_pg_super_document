# DCH_from_char

## Location
[src/backend/utils/adt/formatting.c:3412-3961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L3412-L3961)

## Overview
A static function that parses a formatted date/time string according to format specifications, extracting date/time components into a TmFromChar structure for subsequent processing into PostgreSQL timestamps.

## Definition
```c
static bool DCH_from_char(FormatNode *node, const char *in, TmFromChar *out,
                         Oid collid, bool std, Node *escontext)
```

## Detailed Description
This function is the core parsing engine for converting formatted date/time strings back into structured date/time data. It processes a linked list of format nodes, matching each format specifier against the corresponding portion of the input string. The function handles an extensive range of date/time format codes including years, months, days, hours, minutes, seconds, microseconds, time zones, AM/PM indicators, and various cultural variations. It supports both strict parsing (FX mode) and flexible parsing with automatic whitespace handling.

The function workflow:
1. Caches localized time data for international month/day names
2. Iterates through format nodes and input string simultaneously
3. Handles whitespace skipping in non-FX mode
4. For each format specifier, calls appropriate parsing functions
5. Validates and stores extracted values in the output structure
6. Supports error contexts for soft error handling
7. Handles various parsing modes including strict and flexible matching

## Parameters / Member Variables
- `node`: Linked list of FormatNode structures defining the expected input format
- `in`: Input string containing the formatted date/time data to parse
- `out`: TmFromChar structure to receive the parsed date/time components
- `collid`: Collation ID for locale-specific parsing (month names, etc.)
- `std`: Boolean controlling FX (fixed width) mode - true for strict parsing
- `escontext`: Error handling context - if present, returns false on error instead of throwing

## Dependencies
- Functions called/Symbols referenced:
  - [cache_locale_time](../c/cache_locale_time.md) (for caching localized day/month names)
  - [from_char_seq_search](../f/from_char_seq_search.md) (for parsing textual date/time elements)
  - [from_char_parse_int](../f/from_char_parse_int.md), from_char_parse_int_len (for parsing numeric fields)
  - [from_char_set_int](../f/from_char_set_int.md), from_char_set_mode (for storing parsed values)
  - [adjust_partial_year_to_2020](../a/adjust_partial_year_to_2020.md) (for handling 2-digit years)
  - [DecodeTimezoneAbbrevPrefix](DecodeTimezoneAbbrevPrefix.md) (for timezone abbreviation parsing)
  - Various format validation and utility functions
  - Numerous DCH format constants and parsing macros
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1044)
  - [do_to_timestamp](../d/do_to_timestamp.md) (formatting.c:4742)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Contains an extremely comprehensive switch statement handling dozens of format codes
- Supports both strict (FX) and flexible parsing modes with different whitespace handling
- Handles international date formats through locale-aware parsing
- Includes sophisticated error handling with detailed error messages
- Manages complex parsing scenarios like partial years, timezone offsets, and Roman numerals
- Used primarily by PostgreSQL's TO_TIMESTAMP() and TO_DATE() functions
- The function maintains parsing state and can handle format modifier combinations
- Returns true on successful parsing, false on failure (when using error contexts)
- Supports parsing of both absolute timestamps and intervals depending on context
- Includes special handling for edge cases like leap years, timezone boundaries, etc.

## Simplified Source

```c
static void DCH_from_char(FormatNode *node, const char *in, TmFromChar *out,
                         Oid collid, bool std, Node *escontext) {
    FormatNode *n;
    const char *s;
    int len, value;
    bool fx_mode = std;
    int extra_skip = 0;

    // Cache localized time data for international parsing
    cache_locale_time();

    // Process each format node against input string
    for (n = node, s = in; n->type != NODE_TYPE_END && *s != '\0'; n++) {
        // Skip whitespace in non-FX mode
        if (!fx_mode && should_skip_whitespace(n)) {
            while (*s != '\0' && isspace(*s)) {
                s++;
                extra_skip++;
            }
        }

        // Handle separators and spaces
        if (n->type == NODE_TYPE_SPACE || n->type == NODE_TYPE_SEPARATOR) {
            handle_separator(n, &s, std, fx_mode, &extra_skip, escontext);
            continue;
        }

        // Handle literal text characters
        else if (n->type != NODE_TYPE_ACTION) {
            handle_literal_char(n, &s, std, fx_mode, &extra_skip, escontext);
            continue;
        }

        // Set parsing mode for this field
        if (!from_char_set_mode(out, n->key->date_mode, escontext))
            return;

        // Parse specific format codes
        switch (n->key->id) {
            case DCH_FX:
                fx_mode = true;
                break;

            // AM/PM indicators
            case DCH_A_M: case DCH_P_M: case DCH_a_m: case DCH_p_m:
            case DCH_AM: case DCH_PM: case DCH_am: case DCH_pm:
                parse_ampm_indicator(n, &s, out, escontext);
                break;

            // Hour formats
            case DCH_HH: case DCH_HH12:
                parse_hour_12(&out->hh, &s, n, escontext);
                out->clock = CLOCK_12_HOUR;
                break;
            case DCH_HH24:
                parse_hour_24(&out->hh, &s, n, escontext);
                break;

            // Minutes and seconds
            case DCH_MI:
                parse_minutes(&out->mi, &s, n, escontext);
                break;
            case DCH_SS:
                parse_seconds(&out->ss, &s, n, escontext);
                break;

            // Fractional seconds
            case DCH_MS:
                parse_milliseconds(&out->ms, &s, n, escontext);
                break;
            case DCH_US: case DCH_FF1: case DCH_FF2: case DCH_FF3:
            case DCH_FF4: case DCH_FF5: case DCH_FF6:
                parse_microseconds(&out->us, &s, n, escontext);
                break;

            // Timezone handling
            case DCH_tz: case DCH_TZ: case DCH_OF:
                parse_timezone(n, &s, out, escontext);
                break;

            // Date components
            case DCH_YYYY: case DCH_IYYY:
                parse_year_4digit(&out->year, &s, n, escontext);
                out->yysz = 4;
                break;
            case DCH_YY: case DCH_IY:
                parse_year_2digit(&out->year, &s, n, escontext);
                out->yysz = 2;
                break;
            case DCH_MM:
                parse_month_numeric(&out->mm, &s, n, escontext);
                break;
            case DCH_MONTH: case DCH_Month: case DCH_month:
                parse_month_name(&out->mm, &s, n, collid, escontext);
                break;
            case DCH_DD:
                parse_day(&out->dd, &s, n, escontext);
                break;
            case DCH_DAY: case DCH_Day: case DCH_day:
                parse_day_name(&out->d, &s, n, collid, escontext);
                break;

            // Other formats...
            default:
                parse_other_formats(n, &s, out, escontext);
                break;
        }

        // Skip trailing spaces after fields in non-FX mode
        if (!fx_mode) {
            skip_trailing_spaces(&s, &extra_skip);
        }
    }

    // Validate complete parsing in standard mode
    if (std) {
        validate_complete_parsing(n, s, escontext);
    }
}
```