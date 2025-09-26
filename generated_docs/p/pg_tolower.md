# pg_tolower

## Location
[src/port/pgstrcasecmp.c:122-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pgstrcasecmp.c#L122-L134)

## Overview
Converts a single character to lowercase in a safe manner that works with both ASCII and extended character sets.

## Definition

```c
unsigned char
pg_tolower(unsigned char ch)
```
## Detailed Description
The  function converts a character to its lowercase equivalent, providing a safe alternative to the standard C library's  function. Like its uppercase counterpart , this function is designed to be safe when applied to characters that are not uppercase letters - it will simply return the character unchanged if it's not an uppercase letter.

For ASCII characters (A-Z), it performs direct conversion by adding the offset between uppercase and lowercase letters. For extended characters with the high bit set, it first checks if the character is uppercase using  before applying the standard  function. This dual approach ensures both efficiency for common ASCII cases and correctness for locale-specific characters.

Similar to , the function includes a note that the approach has limitations for multibyte character sets, as it operates on individual bytes rather than complete multibyte sequences.

## Parameters / Member Variables
- : The unsigned character to convert to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - isupper (standard C library function for locale-aware uppercase detection)
  - tolower (standard C library function for locale-aware case conversion)
- Called from (representative examples):
  - [String](../S/String.md) formatting functions (str_tolower, str_initcap)
  - Date/time parsing (APPEND_CHAR macro, DecodeTimezoneAbbrevPrefix)
  - Pattern matching utilities (SB_lower_char, patternToSQLRegex)
  - libpq field name lookup (PQfnumber)
  - psql keyword case conversion (pg_strdup_keyword_case)
  - [Path](../P/Path.md) comparison (dir_strcmp)

## Notes and Other Information
- Returns the lowercase version of the input character, or the original character if not uppercase
- Safe to call on any character value, including non-letters
- Optimized for ASCII with direct arithmetic conversion
- Uses locale-aware functions for extended character sets
- Limited effectiveness with multibyte character encodings
- Widely used in text processing, parsing, and case-insensitive operations throughout PostgreSQL