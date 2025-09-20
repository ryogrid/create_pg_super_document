# r_mark_ymUs_

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:881-892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L881-L892)

## Overview
Identifies and marks Turkish past participle suffixes ending in -ymUs̈ variants (miş, muş, mış, müş) in the Snowball stemming algorithm for Turkish text processing.

## Definition

```c
}

static int r_mark_ymUs_(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish Snowball stemmer implementation that identifies past participle suffixes with variants of -ymUs̈. It performs pattern matching for the Turkish suffixes "miş", "muş", "mış", and "müş" which are past participle endings in Turkish grammar. The function implements vowel harmony checking and handles the optional 'y' consonant insertion rule that is characteristic of Turkish morphology.

The function uses backward matching from the current cursor position and requires that the matched suffix satisfies Turkish vowel harmony rules. It also checks for proper suffix boundaries and applies the optional 'y' consonant marking rules.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the string
  - : Left boundary limit for matching
  - : Pointer to the character array being processed

## Dependencies
- Functions called/Symbols referenced:
  - : Validates Turkish vowel harmony rules
  - : Performs backward pattern matching against suffix arrays
  - : Handles 'y' consonant insertion rules
  - : Array containing the four -ymUs̈ suffix patterns (miş, muş, mış, müş)

- Called from (representative examples):
  - : Main suffix processing function (multiple locations)

## Notes and Other Information
- Returns 1 on successful match, 0 on failure, negative values for errors
- Checks that cursor position allows for at least 4 characters (minimum suffix length)
- Verifies the last character is 's̈' (Unicode 159) before attempting pattern matching
- Part of the Turkish morphological analysis focusing on past participle recognition
- The suffix variants follow Turkish vowel harmony: front vowels (i, ü) vs back vowels (ı, u)