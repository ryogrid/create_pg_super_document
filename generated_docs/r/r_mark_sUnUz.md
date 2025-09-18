# r_mark_sUnUz

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:822-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L822-L827)

## Overview
A static function in the Turkish stemmer that identifies and marks the suffix "sUnUz" (meaning "you are" plural in Turkish) and its variations, used in Turkish verb conjugation processing.

## Definition
```c
static int r_mark_sUnUz(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer in PostgreSQL's Snowball implementation. It identifies the Turkish suffix "sUnUz" and its phonetic variations ("siniz", "sunuz", "sınız", "sünüz") which represent the second person plural present tense form "you are" (addressing multiple people) in Turkish verbs. Unlike other similar functions, this one doesn't perform vowel harmony checking, as the suffix variations are more fixed in their pattern.

The function works by:
1. Checking if the current character is 'z' (ASCII 122)
2. Ensuring there are at least 4 characters before the current position (z->c - 4 <= z->lb)
3. Using `find_among_b()` to match against the predefined suffix array `a_15`

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor position, and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md)
  - a_15 (static array containing suffix patterns: "siniz", "sunuz", "sınız", "sünüz")
- Called from (representative examples):
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md) (at lines 941, 1052, 1105)

## Notes and Other Information
- The `a_15` array contains 4 suffix variations: "siniz" (5 chars), "sunuz" (5 chars), "sınız" (7 UTF-8 bytes with dotless ı), and "sünüz" (7 UTF-8 bytes with ü)
- This function is simpler than the other marking functions as it doesn't call vowel harmony checking or optional consonant handling
- The minimum length check (z->c - 4 <= z->lb) ensures the word is long enough to contain this suffix
- The function handles both front vowel (i, ü) and back vowel (ı, u) harmony patterns
- The function returns 1 on successful match, 0 on failure
- This is part of the Turkish morphological analysis system used for full-text search indexing in PostgreSQL