# eq_s_b

## Location
[src/backend/snowball/libstemmer/utilities.c:220-224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L220-L224)

## Overview
A utility function in the Snowball stemming algorithm that performs exact string matching and backward cursor movement.

## Definition

```c
}

extern int eq_s_b(struct SN_env * z, int s_size, const symbol * s)
```
## Detailed Description
The  function is a core utility in the Snowball stemming framework that performs backward string matching. It checks if a given string pattern  matches the text at the current cursor position in the Snowball environment, reading backwards from the cursor. If the match is successful, it moves the cursor backward by the length of the matched string.

The function first verifies that there are enough characters between the current cursor position and the left boundary to accommodate the string being matched. It then performs a byte-wise comparison using . If the strings match exactly, the cursor is moved backward and the function returns 1 (success). If there's insufficient space or the strings don't match, it returns 0 (failure).

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the text buffer and cursor positions
- `s_size`: The length of the string pattern to match (number of symbols)
- `*s`: Pointer to the symbol array containing the pattern to match against
## Dependencies
- Functions called/Symbols referenced:
  -  (C standard library function)
  -  (Snowball type definition)
- Called from (representative examples):
  - Various stemming rule functions across multiple language stemmers (Danish, Dutch, Finnish, French, German, Italian, Portuguese, Spanish, Romanian, Russian, Tamil, Turkish, Greek, Nepali, Yiddish)
  -  function in the same utilities file

## Notes and Other Information
- This is a fundamental operation in Snowball stemming algorithms, used extensively across all language-specific stemmer implementations
- The function modifies the cursor position () only on successful matches
- Returns 1 for successful match (with cursor moved), 0 for no match
- Part of the backward-matching family of functions in the Snowball utilities
- Used in suffix removal operations where patterns are matched from right to left

## Simplified Source

```c
extern int eq_s_b(struct SN_env * z, int s_size, const symbol * s) {
    // Check if enough characters exist before current position
    if (z->c - z->lb < s_size) return 0;

    // Compare string pattern backward from current position
    if (memcmp(z->p + z->c - s_size, s, s_size * sizeof(symbol)) != 0) return 0;

    // Match successful, move cursor backward
    z->c -= s_size;
    return 1;
}
```