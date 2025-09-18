# setDecimalLocale

## Location
src/fe_utils/print.c: 3641 - 3676

## Overview
The setDecimalLocale function initializes locale-specific numeric formatting settings by extracting decimal point, thousands separator, and digit grouping information from the system locale for proper number display.

## Definition


## Detailed Description
This function configures the global numeric formatting settings used by PostgreSQL client utilities to display numbers according to the current system locale. It calls localeconv() to retrieve locale-specific numeric formatting information and extracts three key components: the decimal point character, the thousands separator character, and the number of digits per group.

The function implements several fallback mechanisms to ensure valid formatting settings even when the locale provides incomplete information. If the locale doesn't specify a decimal point, it defaults to the SQL standard period ('.'). For thousands separators, if the locale doesn't provide one, the function chooses a comma (',') unless that would conflict with the decimal point, in which case it uses a period. The digit grouping size is validated to be between 1-6 digits, defaulting to 3 (the most common grouping) if the locale value is invalid.

The function also ensures that the thousands separator and decimal point are different characters to avoid formatting ambiguity. This initialization is typically performed once during application startup to establish consistent numeric formatting throughout the program's execution.

## Parameters / Member Variables
- None (void function that operates on global state)

## Dependencies
- Functions called/Symbols referenced:
  - localeconv() (standard C library function to get locale formatting info)
  - [pg_strdup](../p/pg_strdup.md)() (PostgreSQL string duplication function)
  - strcmp() (standard C string comparison)
- Global variables set:
  - decimal_point (global string for decimal point character)
  - thousands_sep (global string for thousands separator character) 
  - groupdigits (global integer for digit grouping size)
- Called from (representative examples):
  - [main](../m/main.md) (psql startup initialization)

## Notes and Other Information
- The function follows the Open Group standard for locale information but applies practical limitations for PostgreSQL's use case
- Only the first grouping value from the locale is considered, ignoring more complex grouping patterns that some locales might specify
- CHAR_MAX values in grouping are ignored as they typically indicate no grouping should be performed
- The digit grouping range check (1-6) prevents unreasonable grouping sizes while accommodating different cultural preferences
- The fallback logic ensures the function always produces usable formatting settings regardless of locale completeness
- Similar locale handling code exists in PostgreSQL's backend formatting.c, indicating this is part of a consistent approach across the system
- The function modifies global state and should typically be called only once during initialization
- Memory for decimal_point and thousands_sep strings is allocated via pg_strdup() and should be considered permanent for the program's lifetime