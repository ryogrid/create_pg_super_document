# DCH_from_char

## Location
src/backend/utils/adt/formatting.c: 3412 - 3961

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
  - cache_locale_time (for caching localized day/month names)
  - from_char_seq_search (for parsing textual date/time elements)
  - from_char_parse_int, from_char_parse_int_len (for parsing numeric fields)
  - from_char_set_int, from_char_set_mode (for storing parsed values)
  - adjust_partial_year_to_2020 (for handling 2-digit years)
  - DecodeTimezoneAbbrevPrefix (for timezone abbreviation parsing)
  - Various format validation and utility functions
  - Numerous DCH format constants and parsing macros
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1044)
  - do_to_timestamp (formatting.c:4742)

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