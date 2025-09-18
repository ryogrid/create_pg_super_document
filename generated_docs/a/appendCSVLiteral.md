# appendCSVLiteral

## Location
src/backend/utils/error/csvlog.c: 37 - 62

## Overview
A static inline function that appends a CSV-formatted version of a string to a StringInfo buffer, using PostgreSQL's default CSV formatting rules.

## Definition


## Detailed Description
This function safely converts a C string into CSV format and appends it to a StringInfo buffer. It implements PostgreSQL's CSV formatting conventions where double quotes (") serve as both quote and escape characters. The function handles NULL input gracefully by appending nothing, and properly escapes any embedded double quotes by doubling them (" becomes ""). All non-NULL strings are wrapped in double quotes regardless of content, ensuring consistent CSV formatting.

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the CSV-formatted string to
- `data`: The input C string to be CSV-formatted and appended (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro (macro for efficiently appending single characters)
- Called from (representative examples):
  - [write_csvlog](../w/write_csvlog.md) (primary caller, used extensively for CSV log formatting)

## Notes and Other Information
- Declared as static inline for performance optimization since it's frequently called
- NULL input results in no output (empty field in CSV)
- Uses PostgreSQL's standard CSV quoting convention with double quotes
- Escapes embedded quotes by doubling them (CSV standard)
- All strings are quoted regardless of content for consistency
- Located in src/backend/utils/error/csvlog.c:37-62