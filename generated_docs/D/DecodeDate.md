# DecodeDate

## Location
src/backend/utils/adt/datetime.c: 2398 - 2507

## Overview
Decodes a date string with delimiters into date components, parsing both textual month names and numeric fields while handling various date formats and field arrangements.

## Definition
```c
static int DecodeDate(char *str, int fmask, int *tmask, bool *is2digits, struct pg_tm *tm)
```

## Detailed Description
This internal function parses a date string containing delimiters (such as '-', '/', '.') and extracts date components. It handles various date formats by employing a two-pass parsing strategy:

1. **First pass**: Identifies and processes textual fields (like month names) since these are unambiguous
2. **Second pass**: Processes remaining numeric fields using context-sensitive interpretation

The function performs field separation by treating any non-alphanumeric characters as delimiters. It maintains careful field masking to prevent duplicate date components and ensure consistent date representation.

Key features:
- Supports textual month names (January, Feb, etc.)
- Handles numeric date components in various orders
- Performs field validation to prevent conflicts
- Sets appropriate field masks for downstream validation
- Detects 2-digit year usage for Y2K handling

## Parameters / Member Variables
- `str`: The input date string to be parsed (modified during parsing)
- `fmask`: Bitmask indicating which field types have already been seen in outer parsing context
- `tmask`: Output parameter receiving bitmask for field types found in this string
- `is2digits`: Output parameter set to true if a 2-digit year is detected
- `tm`: Output pg_tm structure where decoded date components are stored

## Dependencies
- Functions called/Symbols referenced:
  - pg_tm (struct)
  - fsec_t (type)
  - MAXDATEFIELDS (constant)
  - DecodeSpecial
  - DecodeNumber
  - DTK_M (macro)
  - DTERR_BAD_FORMAT (constant)
  - MONTH, DOY, TZ (field type constants)
  - DTK_DATE_M (mask constant)
- Called from (representative examples):
  - DecodeDateTime
  - DecodeTimeOnly

## Notes and Other Information
- This is a static (internal) function not exposed outside the datetime.c module
- Modifies the input string during parsing by null-terminating field boundaries
- Uses a field array (MAXDATEFIELDS) to manage parsed components
- Prioritizes textual month identification to resolve ambiguity in numeric date formats
- Performs minimal validation - comprehensive validation is deferred to ValidateDate()
- Supports day-of-year (DOY) format in addition to standard month/day format
- Returns 0 on success or DTERR_BAD_FORMAT on parsing errors
- Field masking prevents duplicate date components (e.g., two month fields)
- Located in src/backend/utils/adt/datetime.c:2398-2507