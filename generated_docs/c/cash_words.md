# cash_words

## Location
[src/backend/utils/adt/cash.c:959-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L959-L1045)

## Overview
A PostgreSQL function that converts a Cash value into its English textual representation, expressing monetary amounts in written words with proper pluralization and formatting.

## Definition


## Detailed Description
This function converts a Cash value into a human-readable English text representation, such as "One hundred twenty-three dollars and forty-five cents". The function handles negative values by prefixing "minus" and works with large amounts up to quadrillions. It breaks down the monetary value into different magnitude groups (cents, hundreds, thousands, millions, billions, trillions, quadrillions) and converts each group using the num_word helper function. The output follows North American currency conventions with proper singular/plural forms for "dollar"/"dollars" and "cent"/"cents". The final output is capitalized and returned as a PostgreSQL text datum.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: PostgreSQL's standard function argument structure containing:
  - **value (Cash)**: The cash value to convert to words

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call
  - [num_word](../n/num_word.md): Converts numeric values to English words
  - INT64CONST: Macro for 64-bit integer constants
  - [pg_toupper](../p/pg_toupper.md): Converts character to uppercase
  - cstring_to_text: Converts C string to PostgreSQL text type
  - PG_RETURN_TEXT_P: Returns text result
  - Cash: Cash data type for internal variables
  - strcat, strcpy: Standard C string manipulation functions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:959-1045
- Explicitly noted as "North American centric" in the source comments
- Handles values up to quadrillions (10^15 range)
- Uses a 256-character buffer for building the output string
- Properly handles edge cases like zero values and negative amounts
- Implements correct pluralization logic for currency units
- The function assumes North American dollar/cent terminology and conventions
- Returns capitalized text suitable for formal documents or checks