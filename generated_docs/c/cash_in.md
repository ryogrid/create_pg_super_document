# cash_in

## Location
[src/backend/utils/adt/cash.c:173-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L173-L386)

## Overview
A PostgreSQL input function that converts a string representation to a Cash data type, supporting various currency formats and localization.

## Definition
```c
Datum cash_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function parses string representations of monetary values and converts them to PostgreSQL's internal Cash data type. It supports flexible input formats including currency symbols, thousands separators, decimal points, and various sign representations (leading/trailing minus signs, parentheses for negative values). The function uses locale-specific settings from the system's locale configuration to handle different currency formats appropriately. It performs comprehensive input validation, overflow detection, and proper rounding when necessary.

The function accumulates the absolute value in negative form to handle the full range of signed integers more safely, then applies the correct sign at the end. It supports formats like: $123.45, 123,456.78, (123.45) for negative, +123.45, -123.45, etc.

## Parameters / Member Variables
- Internal variables:
  - `result`: Final Cash value to return
  - `value`: Accumulated value during parsing (built in negative)
  - `dec`: Count of decimal places processed
  - `sgn`: Sign multiplier (1 for positive, -1 for negative)
  - `seen_dot`: Boolean flag tracking if decimal point encountered
  - `fpoint`: Number of fractional digits from locale
  - `dsymbol`: Decimal point character from locale
  - `ssymbol`, `psymbol`, `nsymbol`, `csymbol`: Locale-specific symbols

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - [PGLC_localeconv](../P/PGLC_localeconv.md) (locale conversion settings)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (safe multiplication with overflow detection)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (safe subtraction with overflow detection)
  - ereturn (soft error return mechanism)
  - PG_RETURN_CASH (return macro for Cash values)
  - PG_INT64_MIN (minimum 64-bit integer constant)
- Called from:
  - This appears to be a top-level input function, likely called by PostgreSQL's type system

## Notes and Other Information
- This is a PostgreSQL-style input function following the fmgr (function manager) calling convention
- Uses comprehensive locale support for international currency formats
- Implements robust overflow detection throughout the parsing process
- Handles edge cases like the most negative 64-bit integer value
- Supports both hard and soft error reporting modes via error context
- Includes debug output when compiled with CASHDEBUG flag
- Performs input validation and normalization of whitespace and currency symbols
- Uses safe arithmetic operations to prevent integer overflow vulnerabilities
- The parsing logic builds values in negative form as a safety measure against integer overflow

## Simplified Source

```c
Datum
cash_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Cash result;
    Cash value = 0;
    Cash dec = 0;
    Cash sgn = 1;
    bool seen_dot = false;
    const char *s = str;
    int fpoint;
    char dsymbol;
    const char *ssymbol, *psymbol, *nsymbol, *csymbol;
    struct lconv *lconvert = PGLC_localeconv();

    // Get locale-specific formatting info
    fpoint = lconvert->frac_digits;
    if (fpoint < 0 || fpoint > 10)
        fpoint = 2;    // Default to 2 decimal places

    // Set up locale symbols with fallbacks
    dsymbol = (*lconvert->mon_decimal_point != '\0' && lconvert->mon_decimal_point[1] == '\0')
              ? *lconvert->mon_decimal_point : '.';
    ssymbol = (*lconvert->mon_thousands_sep != '\0') ? lconvert->mon_thousands_sep :
              (dsymbol != ',') ? "," : ".";
    csymbol = (*lconvert->currency_symbol != '\0') ? lconvert->currency_symbol : "$";
    psymbol = (*lconvert->positive_sign != '\0') ? lconvert->positive_sign : "+";
    nsymbol = (*lconvert->negative_sign != '\0') ? lconvert->negative_sign : "-";

    // Skip leading whitespace and currency symbol
    while (isspace((unsigned char) *s)) s++;
    if (strncmp(s, csymbol, strlen(csymbol)) == 0) s += strlen(csymbol);
    while (isspace((unsigned char) *s)) s++;

    // Handle leading sign
    if (strncmp(s, nsymbol, strlen(nsymbol)) == 0) {
        sgn = -1;
        s += strlen(nsymbol);
    } else if (*s == '(') {
        sgn = -1;
        s++;
    } else if (strncmp(s, psymbol, strlen(psymbol)) == 0) {
        s += strlen(psymbol);
    }

    // Skip more whitespace and currency after sign
    while (isspace((unsigned char) *s)) s++;
    if (strncmp(s, csymbol, strlen(csymbol)) == 0) s += strlen(csymbol);
    while (isspace((unsigned char) *s)) s++;

    // Parse digits (accumulate in negative to handle full range safely)
    for (; *s; s++) {
        if (isdigit((unsigned char) *s) && (!seen_dot || dec < fpoint)) {
            int8 digit = *s - '0';

            if (pg_mul_s64_overflow(value, 10, &value) ||
                pg_sub_s64_overflow(value, digit, &value))
                ereturn(escontext, (Datum) 0,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("value \"%s\" is out of range for type %s", str, "money")));

            if (seen_dot) dec++;
        } else if (*s == dsymbol && !seen_dot) {
            seen_dot = true;
        } else if (strncmp(s, ssymbol, strlen(ssymbol)) == 0) {
            s += strlen(ssymbol) - 1;  // Skip thousands separator
        } else {
            break;
        }
    }

    // Round if next digit >= 5
    if (isdigit((unsigned char) *s) && *s >= '5') {
        if (pg_sub_s64_overflow(value, 1, &value))
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("value \"%s\" is out of range for type %s", str, "money")));
    }

    // Pad with zeros if insufficient decimal places
    for (; dec < fpoint; dec++) {
        if (pg_mul_s64_overflow(value, 10, &value))
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("value \"%s\" is out of range for type %s", str, "money")));
    }

    // Skip trailing digits and validate remaining characters
    while (isdigit((unsigned char) *s)) s++;

    while (*s) {
        if (isspace((unsigned char) *s) || *s == ')') {
            s++;
        } else if (strncmp(s, nsymbol, strlen(nsymbol)) == 0) {
            sgn = -1;
            s += strlen(nsymbol);
        } else if (strncmp(s, psymbol, strlen(psymbol)) == 0) {
            s += strlen(psymbol);
        } else if (strncmp(s, csymbol, strlen(csymbol)) == 0) {
            s += strlen(csymbol);
        } else {
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("invalid input syntax for type %s: \"%s\"", "money", str)));
        }
    }

    // Apply sign (check for most negative number overflow)
    if (sgn > 0) {
        if (value == PG_INT64_MIN)
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("value \"%s\" is out of range for type %s", str, "money")));
        result = -value;
    } else {
        result = value;
    }

    PG_RETURN_CASH(result);
}
```