# cash_numeric

## Location
[src/backend/utils/adt/cash.c:1046-1101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L1046-L1101)

## Overview
A PostgreSQL function that converts a Cash value to a Numeric data type, handling locale-specific fractional digit scaling and precision requirements.

## Definition

```c
struct lconv *lconvert = PGLC_localeconv();
```
## Detailed Description
This function converts a Cash value to PostgreSQL's Numeric data type, which provides arbitrary precision decimal arithmetic. The conversion process involves determining the appropriate fractional digits based on the current locale settings, converting the integral cash value to numeric format, and then scaling it appropriately by dividing by the correct power of 10. The function uses locale-specific settings (via PGLC_localeconv()) to determine how many fractional digits should be preserved, with a fallback to 2 digits if the locale settings are invalid. Special care is taken to ensure exact results even with large values approaching INT64_MAX by setting appropriate scale factors before division.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: PostgreSQL's standard function argument structure containing:
  - **money (Cash)**: The cash value to convert to numeric

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call
  - [PGLC_localeconv](../P/PGLC_localeconv.md): Gets locale-specific formatting information
  - [int64_to_numeric](../i/int64_to_numeric.md): Converts 64-bit integer to Numeric type
  - [NumericGetDatum](../N/NumericGetDatum.md): Wraps Numeric value as PostgreSQL Datum
  - [numeric_round](../n/numeric_round.md): Rounds numeric value to specified decimal places
  - [numeric_div](../n/numeric_div.md): Performs division between numeric values
  - DirectFunctionCall2: Calls PostgreSQL functions directly
  - [Int32GetDatum](../I/Int32GetDatum.md): Converts int32 to Datum format
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