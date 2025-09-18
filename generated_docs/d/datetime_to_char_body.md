# datetime_to_char_body

## Location
src/backend/utils/adt/formatting.c: 4181 - 4249

## Overview
Core formatting function that converts date/time or interval data into a formatted string according to a specified format string template.

## Definition
```c
static text *datetime_to_char_body(TmToChar *tmtc, text *fmt, bool is_interval, Oid collid)
```

## Detailed Description
This is the central implementation function for PostgreSQL's date/time formatting functionality. It serves as the core engine for the `to_char()` SQL function family when applied to temporal data types. The function parses a format template string into an internal FormatNode structure, then applies the formatting rules to convert the input time data into a human-readable string representation.

The function implements an intelligent caching mechanism to optimize performance for frequently used format strings. Small format strings (under DCH_CACHE_SIZE) are cached to avoid repeated parsing overhead, while larger format strings bypass the cache to prevent memory bloat.

The actual formatting work is delegated to `DCH_to_char()`, which handles the complex logic of applying format codes to time data. This separation allows `datetime_to_char_body` to focus on format preparation and memory management.

## Parameters / Member Variables
- `tmtc`: Pointer to TmToChar structure containing the time/date data to be formatted
- `fmt`: TEXT object containing the format string template with formatting codes
- `is_interval`: Boolean flag indicating whether the data represents an interval (true) or absolute timestamp (false)
- `collid`: Object ID of the collation to use for string operations

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - parse_format
  - DCH_cache_fetch
  - [DCH_to_char](../D/DCH_to_char.md)
  - cstring_to_text
  - [palloc](../p/palloc.md)/pfree (memory management)
- Called from (representative examples):
  - [timestamp_to_char](../t/timestamp_to_char.md)
  - [timestamptz_to_char](../t/timestamptz_to_char.md)  
  - [interval_to_char](../i/interval_to_char.md)

## Notes and Other Information
- Uses format caching for performance optimization with strings under DCH_CACHE_SIZE
- Allocates result buffer sized at (fmt_len * DCH_MAX_ITEM_SIZ) + 1 to handle worst-case expansion
- Manages memory carefully with proper cleanup of temporary allocations
- Central hub for all PostgreSQL date/time formatting operations
- Format parsing uses DCH_keywords, DCH_suff, DCH_index, and DCH_FLAG configuration arrays