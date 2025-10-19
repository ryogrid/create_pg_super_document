# r_remove_possessive_pronoun

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:135-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L135-L146)

## Overview
A static function in the Indonesian stemmer that removes possessive pronoun suffixes from Indonesian words as part of the morphological stemming process.

## Definition
```c
static int r_remove_possessive_pronoun(struct SN_env * z)
```

## Detailed Description
This function is part of the Snowball stemming algorithm implementation for the Indonesian language. It removes possessive pronoun suffixes by:

1. Setting the 'ket' position to the current cursor position
2. Checking if the word ends with specific characters (97='a' or 117='u')
3. Using backward pattern matching to find possessive pronoun suffixes in the predefined array 'a_1'
4. Removing the matched suffix if found
5. Decrementing a counter (z->I[1]) to track the removal

The function returns 1 on successful removal, 0 if no possessive pronoun suffix is found, or a negative value on error.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with word buffer, cursor positions, and counters

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching function)
  - [slice_del](../s/slice_del.md) (suffix deletion function)
  - a_1 (predefined array of possessive pronoun patterns, containing 3 entries)
- Called from (representative examples):
  - [indonesian_ISO_8859_1_stem](../i/indonesian_ISO_8859_1_stem.md) (at src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:343)
  - [indonesian_UTF_8_stem](../i/indonesian_UTF_8_stem.md) (at src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:343)

## Notes and Other Information
- This is part of PostgreSQL's full-text search capabilities for Indonesian text processing
- The function checks boundary conditions (z->c - 1 <= z->lb) to ensure safe buffer access
- Character codes 97 ('a') and 117 ('u') are checked for optimization - Indonesian possessive pronouns commonly end with these characters
- The counter z->I[1] appears to track the number of suffix removals performed
- Possessive pronouns in Indonesian include suffixes like '-ku' (my), '-mu' (your), '-nya' (his/her/its)

## Simplified Source

```c
static int r_remove_possessive_pronoun(struct SN_env * z) {
    // Set marker at current position
    z->ket = z->c;

    // Quick check: word must end with 'a' or 'u' for possessive pronouns
    if (z->c - 1 <= z->lb || (z->p[z->c - 1] != 97 && z->p[z->c - 1] != 117))
        return 0;

    // Find possessive pronoun pattern (ku, mu, nya)
    if (!(find_among_b(z, a_1, 3))) return 0;

    // Mark start position and remove the suffix
    z->bra = z->c;
    int ret = slice_del(z);
    if (ret < 0) return ret;

    // Track removal count
    z->I[1] -= 1;
    return 1;
}
```