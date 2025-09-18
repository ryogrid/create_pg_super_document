# pg_wc_isword

## Location
src/backend/regex/regc_pg_locale.c: 396 - 404

## Overview
A static function that determines whether a wide character is a word character, defined as alphanumeric characters plus underscore.

## Definition
```c
static int pg_wc_isword(pg_wchar c)
```

## Detailed Description
This function provides a simple but important character classification for regular expression word boundaries and character classes. It implements the commonly-used definition of "word characters" as the set of alphanumeric characters plus the underscore character ('_').

The function operates in two steps:
1. First checks if the character is an underscore, returning 1 (true) immediately if so
2. Otherwise delegates to `pg_wc_isalnum` to determine if the character is alphanumeric

This approach ensures that word character classification follows the same locale-aware strategy as alphanumeric classification, while consistently including underscore regardless of locale settings.

## Parameters / Member Variables
- `c`: A wide character (pg_wchar type) to test for word character classification

## Dependencies
- Functions called/Symbols referenced:
  - CHR (character conversion macro)
  - [pg_wc_isalnum](pg_wc_isalnum.md) (alphanumeric character classification)
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (src/backend/regex/regc_locale.c:606)
  - [cclass_column_index](../c/cclass_column_index.md) (src/backend/regex/regc_locale.c:688)
  - REPLACEARC (src/backend/regex/regcomp.c:255)

## Notes and Other Information
- Static function - only accessible within the same source file
- Returns non-zero (true) if character is a word character, 0 (false) otherwise
- Word characters are defined as: [A-Za-z0-9_] in ASCII, plus locale-appropriate equivalents
- The underscore is always considered a word character regardless of locale
- Inherits locale-awareness from `pg_wc_isalnum` for the alphanumeric portion
- Used in regex character class operations and word boundary detection
- Much simpler implementation than other character classification functions due to delegation to `pg_wc_isalnum`
- Location: src/backend/regex/regc_pg_locale.c:396-404