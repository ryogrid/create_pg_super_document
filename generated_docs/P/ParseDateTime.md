# ParseDateTime

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1598-1779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1598-L1779)

## Overview
ParseDateTime breaks input date/time strings into tokens based on context, identifying field types and extracting individual components for further processing.

## Definition
```c
int ParseDateTime(const char *timestr, char *workbuf, size_t buflen,
                 char **field, int *ftype, int maxfields, int *numfields)
```

## Detailed Description
ParseDateTime is a fundamental tokenization function that breaks down date/time input strings into individual fields with type classification. It processes various formats including numbers, dates, times, text strings, special tokens, and timezone specifications. The function handles multiple delimiters and formats, converts text to lowercase, and assigns specific field types (DTK_NUMBER, DTK_DATE, DTK_TIME, DTK_STRING, DTK_SPECIAL, DTK_TZ) to guide subsequent parsing stages. This is typically the first stage in PostgreSQL's comprehensive date/time parsing pipeline.

## Parameters / Member Variables
- `timestr`: Input date/time string to be tokenized
- `workbuf`: Workspace buffer for storing extracted field strings (must be larger than input)
- `buflen`: Size of the workspace buffer
- `field[]`: Output array of pointers to extracted field strings
- `ftype[]`: Output array of field type indicators (DTK_* constants)
- `maxfields`: Maximum number of fields that can be stored in field[] and ftype[] arrays
- `*numfields`: Output parameter set to actual number of fields detected

## Dependencies
- Functions called/Symbols referenced:
  - DTERR_BAD_FORMAT
  - DTK_TIME, DTK_DATE, DTK_NUMBER, DTK_STRING, DTK_SPECIAL, DTK_TZ (field type constants)
  - [pg_tolower](../p/pg_tolower.md)
  - [datebsearch](../d/datebsearch.md)
  - datetktbl, szdatetktbl (date token table)
- Called from (representative examples):
  - [date_in](../d/date_in.md), time_in, timetz_in
  - [timestamp_in](../t/timestamp_in.md), timestamptz_in
  - [interval_in](../i/interval_in.md)
  - [check_recovery_target_time](../c/check_recovery_target_time.md)
  - ECPG datetime parsing functions

## Notes and Other Information
- Core tokenization function used throughout PostgreSQL's datetime input processing
- Handles complex field type detection including timezone names and embedded delimiters
- Performs case conversion to lowercase for consistent processing
- Field types can hold unexpected items (e.g., DTK_NUMBER can hold date fields like yy.ddd)
- Used by both backend and ECPG client library datetime processing
- Returns 0 on success, DTERR_BAD_FORMAT on invalid input
- Critical for parsing diverse date/time input formats accepted by PostgreSQL

## Simplified Source

```c
int
ParseDateTime(const char *timestr, char *workbuf, size_t buflen,
              char **field, int *ftype, int maxfields, int *numfields)
{
    int nf = 0;
    const char *cp = timestr;
    char *bufp = workbuf;
    const char *bufend = workbuf + buflen;

    // Main tokenization loop
    while (*cp != '\0') {
        // Skip whitespace between fields
        if (isspace((unsigned char) *cp)) {
            cp++;
            continue;
        }

        // Check field limit
        if (nf >= maxfields)
            return DTERR_BAD_FORMAT;

        // Start new field
        field[nf] = bufp;

        // Process different token types
        if (isdigit((unsigned char) *cp)) {
            // Copy initial digits
            *bufp++ = *cp++;
            while (isdigit((unsigned char) *cp))
                *bufp++ = *cp++;

            // Determine field type based on delimiter
            if (*cp == ':') {
                // Time field (HH:MM:SS.fff)
                ftype[nf] = DTK_TIME;
                *bufp++ = *cp++;
                while (isdigit((unsigned char) *cp) || *cp == ':' || *cp == '.')
                    *bufp++ = *cp++;
            } else if (*cp == '-' || *cp == '/' || *cp == '.') {
                // Date field or number
                char delim = *cp;
                *bufp++ = *cp++;

                if (isdigit((unsigned char) *cp)) {
                    ftype[nf] = (delim == '.') ? DTK_NUMBER : DTK_DATE;
                    while (isdigit((unsigned char) *cp))
                        *bufp++ = *cp++;

                    // Look for third field with same delimiter
                    if (*cp == delim) {
                        ftype[nf] = DTK_DATE;
                        *bufp++ = *cp++;
                        while (isdigit((unsigned char) *cp) || *cp == delim)
                            *bufp++ = *cp++;
                    }
                } else {
                    // Date with embedded text (month names)
                    ftype[nf] = DTK_DATE;
                    while (isalnum((unsigned char) *cp) || *cp == delim)
                        *bufp++ = pg_tolower((unsigned char) *cp++);
                }
            } else {
                ftype[nf] = DTK_NUMBER;
            }
        } else if (*cp == '.') {
            // Fractional seconds
            *bufp++ = *cp++;
            while (isdigit((unsigned char) *cp))
                *bufp++ = *cp++;
            ftype[nf] = DTK_NUMBER;
        } else if (isalpha((unsigned char) *cp)) {
            // Text field (month, timezone name, etc.)
            ftype[nf] = DTK_STRING;
            *bufp++ = pg_tolower((unsigned char) *cp++);
            while (isalpha((unsigned char) *cp))
                *bufp++ = pg_tolower((unsigned char) *cp++);

            // Check for timezone or date with embedded separators
            bool is_date = (*cp == '-' || *cp == '/' || *cp == '.');
            if (*cp == '+' || isdigit((unsigned char) *cp)) {
                *bufp = '\0';
                if (datebsearch(field[nf], datetktbl, szdatetktbl) == NULL)
                    is_date = true;
            }

            if (is_date) {
                ftype[nf] = DTK_DATE;
                while (*cp == '+' || *cp == '-' || *cp == '/' ||
                       *cp == '_' || *cp == '.' || *cp == ':' ||
                       isalnum((unsigned char) *cp))
                    *bufp++ = pg_tolower((unsigned char) *cp++);
            }
        } else if (*cp == '+' || *cp == '-') {
            // Signed field (timezone or special)
            *bufp++ = *cp++;

            // Skip whitespace after sign
            while (isspace((unsigned char) *cp))
                cp++;

            if (isdigit((unsigned char) *cp)) {
                // Numeric timezone
                ftype[nf] = DTK_TZ;
                *bufp++ = *cp++;
                while (isdigit((unsigned char) *cp) ||
                       *cp == ':' || *cp == '.' || *cp == '-')
                    *bufp++ = *cp++;
            } else if (isalpha((unsigned char) *cp)) {
                // Special token
                ftype[nf] = DTK_SPECIAL;
                *bufp++ = pg_tolower((unsigned char) *cp++);
                while (isalpha((unsigned char) *cp))
                    *bufp++ = pg_tolower((unsigned char) *cp++);
            } else {
                return DTERR_BAD_FORMAT;
            }
        } else if (ispunct((unsigned char) *cp)) {
            // Skip other punctuation (used as delimiter)
            cp++;
            continue;
        } else {
            return DTERR_BAD_FORMAT;
        }

        // Null-terminate current field and advance
        *bufp++ = '\0';
        nf++;
    }

    *numfields = nf;
    return 0;
}
```