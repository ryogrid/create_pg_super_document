# eq_s

## Location
[src/backend/snowball/libstemmer/utilities.c:215-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L215-L219)

## Overview
The `eq_s` function performs exact string comparison at the current position in a Snowball stemmer environment, advancing the position if the match is successful.

## Definition
```c
extern int eq_s(struct SN_env * z, int s_size, const symbol * s)
```

## Detailed Description
This function is a core utility in the Snowball stemming library for exact string matching operations. It compares a given symbol sequence with the characters at the current position in the stemmer's string buffer. If the comparison succeeds, it advances the current position by the length of the matched string. This is fundamental for pattern matching in stemming algorithms, particularly for identifying specific prefixes, suffixes, or character sequences.

The function performs a bounds check to ensure there are sufficient characters remaining in the buffer, then uses `memcmp` for efficient byte-by-byte comparison of the symbol sequences.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and current position
- `s_size`: The size (length) of the symbol sequence to compare
- `s`: Pointer to the symbol sequence to match against the current buffer position

## Dependencies
- Functions called/Symbols referenced:
  - [SN_env](../S/SN_env.md) struct members (z->c, z->l, z->p)
  - memcmp function for memory comparison
  - symbol type definition
- Called from (representative examples):
  - [r_prelude](../r/r_prelude.md) (in French, German, Serbian, Yiddish stemmers)
  - [r_KER](../r/r_KER.md) (in Indonesian stemmer)
  - [russian_UTF_8_stem](../r/russian_UTF_8_stem.md) (in Russian stemmer)
  - [r_fix_va_start](../r/r_fix_va_start.md) (in Tamil stemmer)
  - [r_mark_regions](../r/r_mark_regions.md) (in Yiddish stemmer)
  - [eq_v](eq_v.md) function

## Notes and Other Information
- Returns 0 if insufficient characters remain or if strings don't match
- Returns 1 if strings match exactly and advances position (z->c += s_size)
- Performs bounds checking: `z->l - z->c < s_size` ensures enough characters are available
- Uses `memcmp` for efficient comparison of symbol sequences
- Critical for exact pattern matching in stemming rules across multiple languages
- Works with both UTF-8 and ISO-8859 encoded text through the symbol abstraction
- The symbol type allows the function to work with different character encodings transparently
- Often used in sequence with other matching functions to implement complex stemming rules