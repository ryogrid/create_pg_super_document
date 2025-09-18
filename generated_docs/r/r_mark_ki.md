# r_mark_ki

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:772-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L772-L776)

## Overview
A static function in the Turkish snowball stemmer that checks for the presence of the suffix "ki" in Turkish words.

## Definition
```c
static int r_mark_ki(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer implementation in PostgreSQL's snowball library. It specifically identifies the Turkish suffix "ki" which is commonly used in Turkish morphology. Unlike other marking functions in the stemmer, this function is straightforward and only performs a simple string match without vowel harmony checking or complex morphological rules.

The function operates by:
1. Using eq_s_b() to check if the current position matches exactly 2 characters from string s_3 (which contains "ki")
2. Returns 1 if the match is found, 0 otherwise

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including the word being processed, current position, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [eq_s_b](../e/eq_s_b.md) (with string s_3 containing "ki")
- Called from (representative examples):
  - [r_stem_suffix_chain_before_ki](r_stem_suffix_chain_before_ki.md)

## Notes and Other Information
- Returns 1 on successful match, 0 on failure
- Part of the Turkish suffix chain stemming process
- The "ki" suffix in Turkish can function as a relativizing particle or demonstrative element
- This is the simplest of the marking functions, requiring no vowel harmony checks
- This function is automatically generated code from snowball stemming algorithms
- Uses exact string matching rather than pattern matching like other suffix functions