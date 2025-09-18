# r_LONG

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:398-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L398-L402)

## Overview
The r_LONG function is a predicate function that detects specific long vowel or vowel sequence patterns in Finnish words during stemming operations.

## Definition
static int r_LONG(struct SN_env * z)

## Detailed Description
This function implements a pattern recognition test for specific long vowel sequences or patterns that are relevant to Finnish morphological processing. It uses backward pattern matching to search for patterns defined in the a_5 array, which contains 7 different patterns that represent long vowels, diphthongs, or other vowel combinations significant to Finnish phonology.

The function serves as a condition check in the Finnish stemming algorithm, typically used to determine whether certain morphological transformations should be applied. Finnish has distinctive long vowels (written as double letters) and complex vowel combinations that affect how suffixes can be modified or removed.

Unlike more complex morphological functions, r_LONG is a simple predicate that either finds a matching pattern (returns 1) or doesn't (returns 0). It performs no text modification itself but provides information used by other functions to make stemming decisions.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position in the string
  - String data and boundaries for pattern matching
- The function uses the a_5 array containing 7 predefined patterns

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function that searches for patterns in a_5 array
  - a_5: Array containing 7 patterns representing long vowel sequences or phonologically significant patterns
- Called from (representative examples):
  - [r_case_ending](r_case_ending.md): Finnish case suffix processing function
  - [r_tidy](r_tidy.md): Finnish word cleanup/normalization function

## Notes and Other Information
This function is part of the Finnish-specific morphological analysis in the Snowball stemming library. The name 'LONG' likely refers to long vowels or vowel sequences that are phonologically significant in Finnish. Finnish distinguishes between short and long vowels, and this distinction affects morphological processes. The function is used as a condition check in more complex morphological transformations, helping to ensure that stemming rules are applied appropriately based on the phonological context. The 7 patterns in a_5 represent the specific vowel sequences that are considered 'long' or otherwise significant for the Finnish stemming algorithm.