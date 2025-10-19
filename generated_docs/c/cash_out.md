# cash_out

## Location
[src/backend/utils/adt/cash.c:387-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L387-L589)

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
  - [PGLC_localeconv](../P/PGLC_localeconv.md) (locale conversion settings)
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf variant)
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

## Simplified Source

```c
Datum
cash_out(PG_FUNCTION_ARGS)
{
    Cash value = PG_GETARG_CASH(0);
    char buf[128];
    char *bufptr;
    int digit_pos, points, mon_group;
    char dsymbol;
    const char *ssymbol, *csymbol, *signsymbol;
    char sign_posn, cs_precedes, sep_by_space;
    struct lconv *lconvert = PGLC_localeconv();

    // Get locale formatting settings with defaults
    points = (lconvert->frac_digits >= 0 && lconvert->frac_digits <= 10) ?
             lconvert->frac_digits : 2;
    mon_group = (*lconvert->mon_grouping > 0 && *lconvert->mon_grouping <= 6) ?
                *lconvert->mon_grouping : 3;

    // Set decimal and thousands separators
    dsymbol = (lconvert->mon_decimal_point[0] && !lconvert->mon_decimal_point[1]) ?
              lconvert->mon_decimal_point[0] : '.';
    ssymbol = (*lconvert->mon_thousands_sep) ? lconvert->mon_thousands_sep :
              ((dsymbol != ',') ? "," : ".");
    csymbol = (*lconvert->currency_symbol) ? lconvert->currency_symbol : "$";

    // Set sign and positioning based on positive/negative value
    if (value < 0) {
        value = -value;
        signsymbol = (*lconvert->negative_sign) ? lconvert->negative_sign : "-";
        sign_posn = lconvert->n_sign_posn;
        cs_precedes = lconvert->n_cs_precedes;
        sep_by_space = lconvert->n_sep_by_space;
    } else {
        signsymbol = lconvert->positive_sign;
        sign_posn = lconvert->p_sign_posn;
        cs_precedes = lconvert->p_cs_precedes;
        sep_by_space = lconvert->p_sep_by_space;
    }

    // Build digit string right-to-left
    bufptr = buf + sizeof(buf) - 1;
    *bufptr = '\0';
    digit_pos = points;

    do {
        // Insert decimal point or thousands separator as needed
        if (points && digit_pos == 0) {
            *(--bufptr) = dsymbol;
        } else if (digit_pos < 0 && (digit_pos % mon_group) == 0) {
            bufptr -= strlen(ssymbol);
            memcpy(bufptr, ssymbol, strlen(ssymbol));
        }

        *(--bufptr) = ((uint64) value % 10) + '0';
        value = ((uint64) value) / 10;
        digit_pos--;
    } while (value || digit_pos >= 0);

    // Format final result based on POSIX sign positioning rules
    char *result;
    switch (sign_posn) {
        case 0: // Parentheses around value and currency
            result = cs_precedes ?
                     psprintf("(%s%s%s)", csymbol, (sep_by_space == 1) ? " " : "", bufptr) :
                     psprintf("(%s%s%s)", bufptr, (sep_by_space == 1) ? " " : "", csymbol);
            break;
        case 1: // Sign precedes value and currency
        default:
            result = cs_precedes ?
                     psprintf("%s%s%s%s%s", signsymbol, (sep_by_space == 2) ? " " : "",
                             csymbol, (sep_by_space == 1) ? " " : "", bufptr) :
                     psprintf("%s%s%s%s%s", signsymbol, (sep_by_space == 2) ? " " : "",
                             bufptr, (sep_by_space == 1) ? " " : "", csymbol);
            break;
        case 2: // Sign follows value and currency
            result = cs_precedes ?
                     psprintf("%s%s%s%s%s", csymbol, (sep_by_space == 1) ? " " : "",
                             bufptr, (sep_by_space == 2) ? " " : "", signsymbol) :
                     psprintf("%s%s%s%s%s", bufptr, (sep_by_space == 1) ? " " : "",
                             csymbol, (sep_by_space == 2) ? " " : "", signsymbol);
            break;
        case 3: // Sign precedes currency symbol
            result = cs_precedes ?
                     psprintf("%s%s%s%s%s", signsymbol, (sep_by_space == 2) ? " " : "",
                             csymbol, (sep_by_space == 1) ? " " : "", bufptr) :
                     psprintf("%s%s%s%s%s", bufptr, (sep_by_space == 1) ? " " : "",
                             signsymbol, (sep_by_space == 2) ? " " : "", csymbol);
            break;
        case 4: // Sign follows currency symbol
            result = cs_precedes ?
                     psprintf("%s%s%s%s%s", csymbol, (sep_by_space == 2) ? " " : "",
                             signsymbol, (sep_by_space == 1) ? " " : "", bufptr) :
                     psprintf("%s%s%s%s%s", bufptr, (sep_by_space == 1) ? " " : "",
                             csymbol, (sep_by_space == 2) ? " " : "", signsymbol);
            break;
    }

    PG_RETURN_CSTRING(result);
}
```