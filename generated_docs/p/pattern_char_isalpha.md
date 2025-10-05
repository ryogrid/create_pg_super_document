# pattern_char_isalpha

## Location
[src/backend/utils/adt/like_support.c:1501-1522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1501-L1522)

## Overview
Determines whether a character is alphabetic and subject to case-folding for pattern matching operations in PostgreSQL's LIKE support.

## Definition
```c
static int pattern_char_isalpha(char c, bool is_multibyte, pg_locale_t locale, bool locale_is_c)
```

## Detailed Description
This function checks whether a given character is a letter and therefore subject to case-folding in pattern matching operations. The function handles various locale configurations and character encodings:

- For C locale: Uses hardcoded ASCII character ranges (A-Z, a-z)
- For multibyte encodings: Assumes any high-bit character is potentially case-varying
- For non-LIBC locale providers: Combines ASCII range checks with high-bit character detection
- For LIBC locale providers: Uses locale-specific `isalpha_l()` function
- Default case: Uses standard `isalpha()` function

The function is designed to work correctly across different PostgreSQL configurations including multibyte character sets and ICU collations, where standard library functions like `isalpha()` may not be suitable.

## Parameters / Member Variables
- `c`: The character to test for alphabetic property
- `is_multibyte`: Boolean indicating if the database uses multibyte character encoding
- `locale`: Pointer to PostgreSQL locale structure containing collation information
- `locale_is_c`: Boolean indicating if the locale is the standard C locale

## Dependencies
- Functions called/Symbols referenced:
  - [pg_locale_t](pg_locale_t.md) (type)
  - IS_HIGHBIT_SET (macro)
  - COLLPROVIDER_LIBC (constant)
  - isalpha_l (function)
  - isalpha (function)
- Called from (representative examples):
  - Pattern_Prefix_Status
  - [like_fixed_prefix](../l/like_fixed_prefix.md)

## Notes and Other Information
- This is a static function within like_support.c, used internally for LIKE pattern processing
- The function avoids using standard library `isalpha()` for multibyte/ICU cases due to potential incompatibilities
- High-bit characters in multibyte encodings are conservatively assumed to be alphabetic
- The function handles different collation providers (LIBC vs ICU) with appropriate logic

## Simplified Source
```c
static int pattern_char_isalpha(char c, bool is_multibyte, pg_locale_t locale, bool locale_is_c) {
    // C locale: use hardcoded ASCII ranges
    if (locale_is_c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    // Multibyte encoding: assume high-bit chars are alphabetic
    else if (is_multibyte && IS_HIGHBIT_SET(c)) {
        return true;
    }

    // Non-LIBC locale provider (e.g., ICU)
    else if (locale && locale->provider != COLLPROVIDER_LIBC) {
        return IS_HIGHBIT_SET(c) ||
               (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    // LIBC locale provider: use locale-specific function
    else if (locale && locale->provider == COLLPROVIDER_LIBC) {
        return isalpha_l((unsigned char) c, locale->info.lt);
    }

    // Default: use standard library function
    else {
        return isalpha((unsigned char) c);
    }
}
```