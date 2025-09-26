# ciprefix

## Location
src/timezone/zic.c: 3640 - 3650

## Overview
A case-insensitive prefix matching function that determines whether one string is an initial prefix of another, used in PostgreSQL's timezone parsing utilities.

## Definition
```c
static bool ciprefix(char const *abbr, char const *word)
```

## Detailed Description
The `ciprefix` function performs case-insensitive prefix matching to determine if the `abbr` string is an initial prefix of the `word` string. Unlike `itsabbr` which allows non-consecutive character matching, `ciprefix` requires exact consecutive character matching from the beginning of both strings. The function uses a do-while loop to compare characters one by one using the `lowerit` function for case-insensitive comparison.

The function returns `true` as soon as it reaches the end of the abbreviation string (indicating a successful prefix match), or `false` if any character doesn't match or if the word ends before the abbreviation is fully matched.

## Parameters / Member Variables
- `abbr`: Pointer to the abbreviation/prefix string to match
- `word`: Pointer to the word string to check for the prefix

## Dependencies
- Functions called/Symbols referenced:
  - lowerit (src/timezone/zic.c:3645)
- Called from (representative examples):
  - byword (src/timezone/zic.c:3664, 3688)

## Notes and Other Information
- Returns `true` if `abbr` is a case-insensitive initial prefix of `word`, `false` otherwise
- Requires consecutive character matching from the beginning of both strings (strict prefix matching)
- Uses the locale-independent `lowerit` function to ensure consistent behavior across different system configurations
- Part of PostgreSQL's timezone compilation utilities (zic)
- Commonly used for matching timezone abbreviations against keywords or longer timezone names
- More restrictive than `itsabbr` as it doesn't allow gaps between matched characters