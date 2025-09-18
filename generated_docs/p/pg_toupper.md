# pg_toupper

## Location
src/port/pgstrcasecmp.c: 105 - 121

## Overview
Converts a single character to uppercase in a safe manner that works with both ASCII and extended character sets.

## Definition


## Detailed Description
The  function converts a character to its uppercase equivalent, providing a safe alternative to the standard C library's  function. Unlike some implementations of , this function is designed to be safe when applied to characters that are not lowercase letters - it will simply return the character unchanged if it's not a lowercase letter.

For ASCII characters (a-z), it performs direct conversion by subtracting the offset between lowercase and uppercase letters. For extended characters with the high bit set, it first checks if the character is lowercase using  before applying the standard  function. This approach ensures both efficiency for common ASCII cases and correctness for locale-specific characters.

The function includes a note that the entire approach is somewhat limited for multibyte character sets, as it operates on individual bytes rather than complete multibyte sequences.

## Parameters / Member Variables
- : The unsigned character to convert to uppercase

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - islower (standard C library function for locale-aware lowercase detection)
  - toupper (standard C library function for locale-aware case conversion)
- Called from (representative examples):
  - String formatting functions (str_toupper, str_initcap)
  - Currency formatting (cash_words)
  - Timezone processing (pg_timezone_abbrevs, pg_tzset)
  - SQL file name processing (PG_SPLIT_WALFILE_NAME_COLS)
  - psql keyword case conversion (pg_strdup_keyword_case)

## Notes and Other Information
- Returns the uppercase version of the input character, or the original character if not lowercase
- Safe to call on any character value, including non-letters
- Optimized for ASCII with direct arithmetic conversion
- Uses locale-aware functions for extended character sets
- Limited effectiveness with multibyte character encodings
- Part of PostgreSQL's character manipulation utilities for consistent text processing