# cash_words

## Location
[src/backend/utils/adt/cash.c:959-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L959-L1045)

## Overview
A PostgreSQL function that converts a Cash value into its English textual representation, expressing monetary amounts in written words with proper pluralization and formatting.

## Definition

```c
struct lconv *lconvert = PGLC_localeconv();
```
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
  - [cstring_to_text](cstring_to_text.md): Converts C string to PostgreSQL text type
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

## Simplified Source

```c
Datum cash_words(PG_FUNCTION_ARGS) {
    Cash value = PG_GETARG_CASH(0);
    uint64 val;
    char buf[256];
    char *p = buf;
    Cash m0, m1, m2, m3, m4, m5, m6;

    // Handle negative values
    if (value < 0) {
        value = -value;
        strcpy(buf, "minus ");
        p += 6;
    } else {
        buf[0] = '\0';
    }

    // Convert to unsigned for safe arithmetic
    val = (uint64) value;

    // Break down into magnitude groups
    m0 = val % 100;                        // cents
    m1 = (val / 100) % 1000;              // hundreds
    m2 = (val / 100000) % 1000;           // thousands
    m3 = (val / 100000000) % 1000;        // millions
    m4 = (val / 100000000000) % 1000;     // billions
    m5 = (val / 100000000000000) % 1000;  // trillions
    m6 = (val / 100000000000000000) % 1000; // quadrillions

    // Convert each non-zero magnitude to words
    if (m6) {
        strcat(buf, num_word(m6));
        strcat(buf, " quadrillion ");
    }
    if (m5) {
        strcat(buf, num_word(m5));
        strcat(buf, " trillion ");
    }
    if (m4) {
        strcat(buf, num_word(m4));
        strcat(buf, " billion ");
    }
    if (m3) {
        strcat(buf, num_word(m3));
        strcat(buf, " million ");
    }
    if (m2) {
        strcat(buf, num_word(m2));
        strcat(buf, " thousand ");
    }
    if (m1) {
        strcat(buf, num_word(m1));
    }

    // Handle zero case
    if (!*p) {
        strcat(buf, "zero");
    }

    // Add dollar/dollars and cent/cents with proper pluralization
    strcat(buf, (val / 100) == 1 ? " dollar and " : " dollars and ");
    strcat(buf, num_word(m0));
    strcat(buf, m0 == 1 ? " cent" : " cents");

    // Capitalize first letter and return as text
    buf[0] = pg_toupper((unsigned char) buf[0]);
    PG_RETURN_TEXT_P(cstring_to_text(buf));
}
```