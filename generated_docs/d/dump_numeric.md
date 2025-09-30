# dump_numeric

## Location
[src/backend/utils/adt/numeric.c:6874-6915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6874-L6915)

## Overview
A debugging utility function that prints detailed information about a Numeric value's internal storage format to stdout.

## Definition
```c
static void dump_numeric(const char *str, Numeric num)
```

## Detailed Description
The `dump_numeric` function is a PostgreSQL debugging utility that provides detailed information about the internal storage format of Numeric values. It prints the weight, scale, sign, and individual digits of a numeric value in a human-readable format. This function is primarily used for development and debugging purposes to understand how numeric values are stored internally.

The function displays the numeric's weight (position of most significant digit), decimal scale (number of digits after decimal point), sign information (including special values like NaN and infinity), and the actual digit array. Each digit is printed with zero-padding according to DEC_DIGITS width.

## Parameters / Member Variables
- `str`: A descriptive string label to prefix the debug output
- `num`: The Numeric value to examine and dump

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_DIGITS
  - NUMERIC_NDIGITS
  - NUMERIC_WEIGHT
  - NUMERIC_DSCALE
  - NUMERIC_SIGN
  - NUMERIC_POS
  - NUMERIC_NEG
  - NUMERIC_NAN
  - NUMERIC_PINF
  - NUMERIC_NINF
  - DEC_DIGITS
  - printf (standard C library)
- Called from (representative examples):
  - NUMERIC_ABBREV_NINF
  - [make_result_opt_error](../m/make_result_opt_error.md)

## Notes and Other Information
- Static function - only accessible within numeric.c
- Used for debugging and development purposes
- Handles all numeric sign types including special values (NaN, +/-Infinity)
- Prints digits with zero-padding for consistent formatting
- Output format: \[label\]: NUMERIC w=\[weight\] d=\[scale\] \[sign\] \[digits...\]
- Essential for understanding PostgreSQL's internal numeric representation
- Not part of the public API - intended for internal debugging only

## Simplified Source

```c
static void dump_numeric(const char *str, Numeric num) {
    NumericDigit *digits = NUMERIC_DIGITS(num);
    int ndigits = NUMERIC_NDIGITS(num);

    // Print header with weight and scale
    printf("%s: NUMERIC w=%d d=%d ", str,
           NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));

    // Print sign information
    switch (NUMERIC_SIGN(num)) {
        case NUMERIC_POS:   printf("POS"); break;
        case NUMERIC_NEG:   printf("NEG"); break;
        case NUMERIC_NAN:   printf("NaN"); break;
        case NUMERIC_PINF:  printf("Infinity"); break;
        case NUMERIC_NINF:  printf("-Infinity"); break;
        default:            printf("SIGN=0x%x", NUMERIC_SIGN(num)); break;
    }

    // Print each digit with zero-padding
    for (int i = 0; i < ndigits; i++) {
        printf(" %0*d", DEC_DIGITS, digits[i]);
    }
    printf("\n");
}
```