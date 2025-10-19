# additional_numeric_locale_len

## Location
[src/fe_utils/print.c:289-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L289-L313)

## Overview
Calculates the additional string length required when formatting a numeric string with locale-aware separators and decimal points.

## Definition

```c
static int
additional_numeric_locale_len(const char *my_str)
```
## Detailed Description
The  function computes how much extra space is needed to format a numeric string with locale-specific formatting. It accounts for two main additions: thousands separators that will be inserted between digit groups, and the replacement of the decimal point character ('.') with the locale-specific decimal point. The function calculates the number of thousands separator instances based on the length of the integral part and the grouping digits setting, then adds space for the locale decimal point if a decimal point exists in the original string.

## Parameters / Member Variables
- `*my_str`: A null-terminated string representing a numeric value to be formatted with locale-specific separators
## Dependencies
- Functions called/Symbols referenced:
  - [integer_digits](../i/integer_digits.md)
  - strlen (standard C library function)
  - strchr (standard C library function)
- Global variables referenced:
  - groupdigits
  - thousands_sep
  - decimal_point
- Called from (representative examples):
  - [format_numeric_locale](../f/format_numeric_locale.md)

## Notes and Other Information
- This is a static function, only accessible within src/fe_utils/print.c
- The function assumes that , , and  are globally available variables containing locale formatting information
- The calculation for thousands separators uses the formula: 
- For decimal points, it subtracts 1 because it's replacing the existing '.' character with the locale-specific decimal point
- Returns 0 if no additional length is needed

## Simplified Source

```c
static int additional_numeric_locale_len(const char *my_str) {
    int int_len = integer_digits(my_str);
    int len = 0;

    // Add space for thousands separators
    if (int_len > groupdigits) {
        len += ((int_len - 1) / groupdigits) * strlen(thousands_sep);
    }

    // Add space for locale decimal point (replacing '.')
    if (strchr(my_str, '.') != NULL) {
        len += strlen(decimal_point) - 1;
    }

    return len;
}
```