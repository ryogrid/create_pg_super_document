# EncodeDateOnly

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:669-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L669-L721)

## Overview
Encodes a date as a formatted string according to the specified style, supporting various date output formats including ISO, SQL, German, and PostgreSQL styles.

## Definition

```c
void
EncodeDateOnly(struct tm *tm, int style, char *str, bool EuroDates)
```
## Detailed Description
EncodeDateOnly converts a date structure (pg_tm) into a formatted string representation based on the specified style parameter. The function supports multiple date formats:

- **USE_ISO_DATES/USE_XSD_DATES**: ISO 8601 format (YYYY-MM-DD)
- **USE_SQL_DATES**: Oracle/Ingres compatible format (MM/DD/YYYY or DD/MM/YYYY based on DateOrder)
- **USE_GERMAN_DATES**: German-style format (DD.MM.YYYY)
- **USE_POSTGRES_DATES**: Traditional PostgreSQL format (MM-DD-YYYY or DD-MM-YYYY based on DateOrder)

The function handles BC dates (years <= 0) by appending " BC" to the formatted string. Year values are adjusted for BC dates using the formula -(tm_year - 1) to properly represent historical dates.

## Parameters / Member Variables
- : Pointer to pg_tm structure containing date components (year, month, day)
- : Integer specifying the output format style (USE_ISO_DATES, USE_SQL_DATES, etc.)
- : Output buffer where the formatted date string will be written

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ultostr_zeropad](../p/pg_ultostr_zeropad.md) (for zero-padded number formatting)
  - MONTHS_PER_YEAR (constant for month validation)
  - DateOrder (global variable controlling day/month ordering)
  - Various style constants (USE_ISO_DATES, USE_SQL_DATES, USE_GERMAN_DATES, USE_POSTGRES_DATES)
  - DATEORDER_DMY (constant for date ordering)

- Called from (representative examples):
  - [date_out](../d/date_out.md) (src/backend/utils/adt/date.c:198)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md) (src/backend/utils/adt/json.c:322)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md) (src/backend/utils/adt/xml.c:2558)
  - [PGTYPESdate_to_asc](../P/PGTYPESdate_to_asc.md) (src/interfaces/ecpg/pgtypeslib/datetime.c:110)

## Notes and Other Information
- The function assumes tm_mon is valid (1-12) and includes an assertion to verify this
- BC dates are handled specially with negative year adjustment and " BC" suffix
- The output buffer must be large enough to accommodate the formatted string plus null terminator
- Date ordering (DMY vs MDY) affects SQL and PostgreSQL style formatting
- Zero-padding is used consistently across all formats for consistent field widths

## Simplified Source

```c
void EncodeDateOnly(struct pg_tm *tm, int style, char *str) {
    // Validate month range
    Assert(tm->tm_mon >= 1 && tm->tm_mon <= MONTHS_PER_YEAR);

    // Format year handling BC dates
    int display_year = (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1);

    switch (style) {
        case USE_ISO_DATES:
        case USE_XSD_DATES:
            // ISO format: YYYY-MM-DD
            str = pg_ultostr_zeropad(str, display_year, 4);
            *str++ = '-';
            str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '-';
            str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            break;

        case USE_SQL_DATES:
            // SQL format: MM/DD/YYYY or DD/MM/YYYY based on DateOrder
            if (DateOrder == DATEORDER_DMY) {
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
                *str++ = '/';
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            } else {
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
                *str++ = '/';
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            }
            *str++ = '/';
            str = pg_ultostr_zeropad(str, display_year, 4);
            break;

        case USE_GERMAN_DATES:
            // German format: DD.MM.YYYY
            str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            *str++ = '.';
            str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            *str++ = '.';
            str = pg_ultostr_zeropad(str, display_year, 4);
            break;

        case USE_POSTGRES_DATES:
        default:
            // PostgreSQL format: MM-DD-YYYY or DD-MM-YYYY based on DateOrder
            if (DateOrder == DATEORDER_DMY) {
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
                *str++ = '-';
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            } else {
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
                *str++ = '-';
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            }
            *str++ = '-';
            str = pg_ultostr_zeropad(str, display_year, 4);
            break;
    }

    // Add BC suffix for historical dates
    if (tm->tm_year <= 0) {
        memcpy(str, " BC", 3);
        str += 3;
    }
    *str = '\0';
}
```