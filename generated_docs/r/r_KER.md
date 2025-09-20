# r_KER

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:188-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L188-L193)

## Overview
Tests whether the current position follows the pattern of a consonant followed by 'er', used as a validation condition in Indonesian prefix removal operations.

## Definition

```c
}

static int r_KER(struct SN_env * z)
```
## Detailed Description
This function validates a specific morphological pattern in Indonesian words where a consonant is followed by 'er'. It first uses  to check that the current character is NOT a vowel (i.e., it's a consonant), then uses  to verify that the next 2 characters match the string 'er' (stored in s_0). This pattern recognition is essential for correct morphological analysis during Indonesian stemming, particularly when processing prefixes that should only be removed under specific phonological conditions.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemmer environment and current position

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping](../o/out_grouping.md) (Snowball framework function for testing characters outside a group)
  - [eq_s](../e/eq_s.md) (Snowball framework function for string equality testing)
  - g_vowel (character grouping array defining Indonesian vowels)
  - s_0 (string constant containing 'er')
- Called from (representative examples):
  - Used as validation function in a_4 array for 'be' prefix pattern

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 if pattern matches (consonant + 'er'), 0 if pattern doesn't match
- The s_0 constant is defined as { 'e', 'r' } in the stemmer
- Used specifically in second-order prefix removal logic where 'be' prefix requires the remaining word to start with consonant+'er'
- Generated automatically by Snowball compiler from Indonesian stemming rules