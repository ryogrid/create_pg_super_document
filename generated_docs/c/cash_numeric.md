# cash_numeric

## Location
src/backend/utils/adt/cash.c: 1046 - 1101

## Overview
A PostgreSQL function that converts a Cash value to a Numeric data type, handling locale-specific fractional digit scaling and precision requirements.

## Definition


## Detailed Description
This function converts a Cash value to PostgreSQL's Numeric data type, which provides arbitrary precision decimal arithmetic. The conversion process involves determining the appropriate fractional digits based on the current locale settings, converting the integral cash value to numeric format, and then scaling it appropriately by dividing by the correct power of 10. The function uses locale-specific settings (via PGLC_localeconv()) to determine how many fractional digits should be preserved, with a fallback to 2 digits if the locale settings are invalid. Special care is taken to ensure exact results even with large values approaching INT64_MAX by setting appropriate scale factors before division.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: PostgreSQL's standard function argument structure containing:
  - **money (Cash)**: The cash value to convert to numeric

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call
  - PGLC_localeconv: Gets locale-specific formatting information
  - int64_to_numeric: Converts 64-bit integer to Numeric type
  - NumericGetDatum: Wraps Numeric value as PostgreSQL Datum
  - numeric_round: Rounds numeric value to specified decimal places
  - numeric_div: Performs division between numeric values
  - DirectFunctionCall2: Calls PostgreSQL functions directly
  - Int32GetDatum: Converts int32 to Datum format
  - PG_RETURN_DATUM: Returns the resulting Datum
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:1046-1101
- Uses locale-aware fractional digit handling with frac_digits from locale settings
- Falls back to 2 fractional digits if locale settings are invalid (< 0 or > 10)
- Implements careful scaling to avoid precision loss with large values near INT64_MAX
- Performs exact division by ensuring proper scale factors are set before division
- The conversion preserves the monetary precision defined by the current locale
- Essential for interoperability between Cash and Numeric data types in SQL operations