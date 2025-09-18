# NUM_prepare_locale

## Location
src/backend/utils/adt/formatting.c: 5287 - 5368

## Overview
Prepares locale-specific formatting information for numeric processing in PostgreSQL's formatting system.

## Definition


## Detailed Description
This function initializes locale-specific formatting information in a NUMProc structure. It retrieves locale information from the system and sets appropriate values for number formatting elements like decimal points, thousands separators, positive/negative signs, and currency symbols. The function handles both locale-aware formatting (when  flag is set) and provides default values for non-locale-aware formatting.

The function addresses specific locale issues, such as broken glibc pt_BR locale that has a comma for decimal but empty thousands separator, ensuring consistent formatting behavior across different locales.

## Parameters / Member Variables
- : Pointer to NUMProc structure that will be populated with locale information
  - : Flag indicating whether locale-specific formatting is needed
  - : Locale-specific negative number sign
  - : Locale-specific positive number sign  
  - : Decimal point character
  - : Thousands separator character
  - : Currency symbol string

## Dependencies
- Functions called/Symbols referenced:
  - [PGLC_localeconv](../P/PGLC_localeconv.md) (gets locale conversion information)
  - IS_LDECIMAL (checks if locale decimal point should be used)
  - strcmp (string comparison for separator validation)
- Called from (representative examples):
  - [NUM_processor](NUM_processor.md) (formatting.c:5987)
  - DCH_ZONED (formatting.c:1077)

## Notes and Other Information
- Only processes locale information when  is true
- Provides fallback default values ("-", "+", ".", ",", " ") when locale is not needed
- Includes special handling for broken locales where thousands separator conflicts with decimal point
- Ensures thousands separator doesn't match decimal point symbol to avoid confusion
- Part of PostgreSQL's comprehensive number formatting system used by to_char() and related functions
- The function safely handles cases where locale information might be NULL or empty