# is_valid_unicode_codepoint

## Location
src/include/mb/pg_wchar.h: 535 - 540

## Overview
Validates whether a given wide character value represents a valid Unicode code point within the allowed Unicode range.

## Definition

```c
static inline bool
is_valid_unicode_codepoint(pg_wchar c)
```
## Detailed Description
This inline function performs a simple range check to determine if a given PostgreSQL wide character () represents a valid Unicode code point. The function validates that the code point falls within the standard Unicode range of U+0001 to U+10FFFF. The function explicitly excludes U+0000 (null character) and any values above the maximum Unicode code point.

The validation follows the Unicode standard which defines the valid code point range as 0x000001 to 0x10FFFF, covering all 17 Unicode planes (Basic Multilingual Plane plus 16 supplementary planes).

## Parameters / Member Variables
- : The wide character value to validate as a Unicode code point

## Dependencies
- Functions called/Symbols referenced: None (simple arithmetic comparison)
- Called from (representative examples):
  - check_unicode_value (src/backend/parser/parser.c:344)
  - unistr (src/backend/utils/adt/varlena.c:6538, 6573, 6608)
  - pg_unicode_to_server (src/backend/utils/mb/mbutils.c:874)
  - pg_unicode_to_server_noerror (src/backend/utils/mb/mbutils.c:934)

## Notes and Other Information
- Defined as a static inline function for performance efficiency
- Located in the header file src/include/mb/pg_wchar.h alongside other Unicode utility functions
- The function excludes U+0000 by using  rather than 
- The upper bound 0x10FFFF represents the highest valid Unicode code point as defined by the Unicode standard
- Used extensively in PostgreSQL's Unicode string processing and character encoding conversion routines