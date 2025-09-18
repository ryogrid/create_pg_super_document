# EncodeDateOnly

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 669 - 721

## Overview
Encodes a date as a formatted string according to the specified style, supporting various date output formats including ISO, SQL, German, and PostgreSQL styles.

## Definition


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
  - pg_ultostr_zeropad (for zero-padded number formatting)
  - MONTHS_PER_YEAR (constant for month validation)
  - DateOrder (global variable controlling day/month ordering)
  - Various style constants (USE_ISO_DATES, USE_SQL_DATES, USE_GERMAN_DATES, USE_POSTGRES_DATES)
  - DATEORDER_DMY (constant for date ordering)

- Called from (representative examples):
  - date_out (src/backend/utils/adt/date.c:198)
  - JsonEncodeDateTime (src/backend/utils/adt/json.c:322)
  - map_sql_value_to_xml_value (src/backend/utils/adt/xml.c:2558)
  - PGTYPESdate_to_asc (src/interfaces/ecpg/pgtypeslib/datetime.c:110)

## Notes and Other Information
- The function assumes tm_mon is valid (1-12) and includes an assertion to verify this
- BC dates are handled specially with negative year adjustment and " BC" suffix
- The output buffer must be large enough to accommodate the formatted string plus null terminator
- Date ordering (DMY vs MDY) affects SQL and PostgreSQL style formatting
- Zero-padding is used consistently across all formats for consistent field widths