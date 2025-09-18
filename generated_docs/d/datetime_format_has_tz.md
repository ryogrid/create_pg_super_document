# datetime_format_has_tz

## Location
src/backend/utils/adt/formatting.c: 4618 - 4680

## Overview
Analyzes a datetime format string to determine whether it contains timezone specifier components.

## Definition
```c
bool datetime_format_has_tz(const char *fmt_str)
```

## Detailed Description
The `datetime_format_has_tz` function parses a datetime format string and returns a boolean indicating whether the format contains timezone-related specifiers. This utility function is essential for determining the appropriate data type and parsing behavior before actually processing datetime input strings.

The function uses PostgreSQL's datetime format caching system for efficiency. For format strings within the cache size limit (DCH_CACHE_SIZE), it leverages cached parsed format entries. For larger format strings, it allocates temporary memory and parses the format directly without caching.

The core logic involves parsing the format string into FormatNode structures using `parse_format`, then analyzing the parsed nodes with `DCH_datetime_type` to determine what datetime components are present. The function specifically checks for the DCH_ZONED flag to identify timezone components.

## Parameters / Member Variables
- `fmt_str` (const char*): The datetime format string to analyze for timezone components

## Dependencies
- Functions called/Symbols referenced:
  - `strlen` - Calculate format string length
  - [palloc](../p/palloc.md) - Allocate memory for large format strings
  - `parse_format` - Parse format string into FormatNode structures
  - `DCH_cache_fetch` - Retrieve cached format entry for reusable formats
  - `DCH_datetime_type` - Analyze parsed format to determine datetime component types
  - [pfree](../p/pfree.md) - Free allocated memory for uncached formats
  - `DCH_CACHE_SIZE` - Maximum size for cached format strings
  - `DCH_ZONED` - Flag indicating timezone components
- Called from (representative examples):
  - [jspIsMutableWalker](../j/jspIsMutableWalker.md) - JSON path mutability analysis
  - Header definitions in formatting.h

## Notes and Other Information
- The function implements a two-tier strategy: using cached format entries for common/small format strings and dynamic parsing for larger ones
- Memory management is carefully handled - temporary allocations are freed for uncached formats while cached formats reuse existing memory
- The DCH_ZONED flag is used as the definitive indicator of timezone presence in the format
- This function supports optimizations in datetime processing by allowing pre-analysis of format requirements
- Part of PostgreSQL's datetime formatting system's format analysis utilities
- The caching mechanism improves performance for frequently used format strings
- Returns true if any timezone specifier (like TZ, OF, etc.) is found in the format string