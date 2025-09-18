# cash_out

## Location
src/backend/utils/adt/cash.c: 387 - 589

## Overview
A PostgreSQL output function that converts a Cash data type to its string representation, formatting it according to the system's locale-specific monetary conventions.

## Definition
```c
Datum cash_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function takes a Cash value as input and produces a formatted string representation following the lc_monetary locale settings. It handles complex formatting rules including currency symbol placement, sign positioning, thousands separators, decimal points, and spacing according to POSIX locale specifications. The function builds the numeric portion right-to-left in a buffer, then applies the appropriate currency symbol and sign formatting based on locale-specific rules.

The formatting follows POSIX locale conventions for monetary display, supporting various international currency formats. It handles both positive and negative values with different formatting rules for each, and can produce formats like $123.45, -$123.45, ($123.45), 123.45$, etc., depending on locale settings.

## Parameters / Member Variables
- Takes a Cash value through PostgreSQL's function argument system (PG_GETARG_CASH)
- Internal variables:
  - `value`: The Cash value being converted
  - `result`: Final formatted string to return
  - `buf[128]`: Working buffer for digit construction
  - `bufptr`: Pointer for building string right-to-left
  - `digit_pos`: Current digit position relative to decimal point
  - `points`: Number of fractional digits from locale
  - `mon_group`: Grouping size for thousands separator
  - `dsymbol`: Decimal point character from locale
  - `ssymbol`, `csymbol`, `signsymbol`: Locale-specific formatting symbols
  - `sign_posn`, `cs_precedes`, `sep_by_space`: POSIX formatting control values

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - PG_GETARG_CASH (argument extraction macro)
  - PGLC_localeconv (locale conversion settings)
  - psprintf (PostgreSQL's sprintf variant)
  - PG_RETURN_CSTRING (return macro for C-strings)
- Called from:
  - This appears to be a top-level output function, likely called by PostgreSQL's type system

## Notes and Other Information
- This is a PostgreSQL-style output function following the fmgr (function manager) calling convention
- Implements full POSIX locale support for international monetary formatting
- Handles edge cases in locale data validation (similar to cash_in for frac_digits and mon_grouping)
- Uses a right-to-left digit building approach for efficiency
- Supports complex formatting with 5 different sign positioning modes (0-4) as defined by POSIX
- Handles currency symbol placement (before/after) and spacing rules
- The formatting logic accommodates various international monetary display conventions
- Builds digits in a fixed-size buffer (128 bytes) which is sufficient for 64-bit integer values
- Uses safe string operations and PostgreSQL's memory management for the final result