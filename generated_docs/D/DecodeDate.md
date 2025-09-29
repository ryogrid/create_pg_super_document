# DecodeDate

## Location
[src/backend/utils/adt/datetime.c:2398-2507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L2398-L2507)

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
  - [pg_tm](../p/pg_tm.md) (struct)
  - fsec_t (type)
  - MAXDATEFIELDS (constant)
  - [DecodeSpecial](DecodeSpecial.md)
  - [DecodeNumber](DecodeNumber.md)
  - DTK_M (macro)
  - DTERR_BAD_FORMAT (constant)
  - MONTH, DOY, TZ (field type constants)
  - DTK_DATE_M (mask constant)
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)

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

## Simplified Source
```c
static int DecodeDate(char *str, int fmask, int *tmask, bool *is2digits, struct pg_tm *tm) {
    fsec_t fsec;
    int nf = 0;
    int i, len, dterr;
    bool haveTextMonth = false;
    int type, val, dmask = 0;
    char *field[MAXDATEFIELDS];

    *tmask = 0;

    // Parse string into fields, using non-alphanumeric chars as separators
    while (*str != '\0' && nf < MAXDATEFIELDS) {
        // Skip separators
        while (*str != '\0' && !isalnum((unsigned char) *str))
            str++;

        if (*str == '\0')
            return DTERR_BAD_FORMAT;

        // Extract field (digits or letters)
        field[nf] = str;
        if (isdigit((unsigned char) *str)) {
            while (isdigit((unsigned char) *str))
                str++;
        } else if (isalpha((unsigned char) *str)) {
            while (isalpha((unsigned char) *str))
                str++;
        }

        // Null-terminate field and advance
        if (*str != '\0')
            *str++ = '\0';
        nf++;
    }

    // First pass: process textual fields (month names)
    for (i = 0; i < nf; i++) {
        if (isalpha((unsigned char) *field[i])) {
            type = DecodeSpecial(i, field[i], &val);
            if (type == IGNORE_DTF)
                continue;

            dmask = DTK_M(type);
            if (type == MONTH) {
                tm->tm_mon = val;
                haveTextMonth = true;
            } else {
                return DTERR_BAD_FORMAT;
            }

            // Check for duplicate fields
            if (fmask & dmask)
                return DTERR_BAD_FORMAT;

            fmask |= dmask;
            *tmask |= dmask;
            field[i] = NULL;  // Mark as processed
        }
    }

    // Second pass: process remaining numeric fields
    for (i = 0; i < nf; i++) {
        if (field[i] == NULL)
            continue;

        len = strlen(field[i]);
        if (len <= 0)
            return DTERR_BAD_FORMAT;

        // Decode numeric field based on context
        dterr = DecodeNumber(len, field[i], haveTextMonth, fmask,
                           &dmask, tm, &fsec, is2digits);
        if (dterr)
            return dterr;

        // Check for duplicate fields
        if (fmask & dmask)
            return DTERR_BAD_FORMAT;

        fmask |= dmask;
        *tmask |= dmask;
    }

    // Verify we have complete date (allowing DOY and TZ as optional)
    if ((fmask & ~(DTK_M(DOY) | DTK_M(TZ))) != DTK_DATE_M)
        return DTERR_BAD_FORMAT;

    return 0;  // Success - validation deferred to ValidateDate()
}
```