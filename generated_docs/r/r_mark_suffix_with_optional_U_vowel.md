# r_mark_suffix_with_optional_U_vowel

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:614-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L614-L645)

## Overview
This function handles the morphological analysis of Turkish suffixes that may optionally include a 'U' vowel (representing high vowels with harmony), implementing vowel insertion rules for Turkish stemming.

## Definition

```c
}

static int r_mark_suffix_with_optional_U_vowel(struct SN_env * z)
```
## Detailed Description
The  function is designed to handle Turkish morphological patterns where a high vowel ('U' representing the archiphoneme that can surface as 'ı', 'i', 'u', or 'ü' depending on vowel harmony) may be optionally inserted in certain suffix contexts. This function differs from the consonant insertion functions in that it deals with vowel insertion rather than consonant insertion.

The function implements a pattern where:
1. First, it checks if there is a 'U' vowel (from the g_U group: high vowels) at the current position preceded by a consonant - representing cases where the optional vowel is present
2. If no 'U' vowel is found, it validates the phonological constraints for cases where the vowel is absent - ensuring no double vowel occurrence and that the preceding character is a consonant

This type of vowel insertion is crucial in Turkish morphology for maintaining syllable structure and avoiding consonant clusters that would violate Turkish phonotactics. The 'U' represents an epenthetic (inserted) vowel that helps break up consonant clusters.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the text buffer, current position, and morphological analysis state
## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b_U](../i/in_grouping_b_U.md) (called 2 times for high vowel group checking)
  - [out_grouping_b_U](../o/out_grouping_b_U.md) (called 2 times for non-vowel checking)
  - [skip_b_utf8](../s/skip_b_utf8.md) (for UTF-8 character boundary handling)
  - g_U (high vowel group: ı, i, u, ü)
  - g_vowel (general vowel group)

- Called from (representative examples):
  - [r_mark_possessives](r_mark_possessives.md) (possessive suffix marking function)

## Notes and Other Information
- Returns 1 if the optional 'U' vowel pattern is valid, 0 otherwise
- Handles vowel insertion rather than consonant insertion, making it unique among the optional marking functions
- The 'U' represents an archiphoneme that surfaces as different high vowels based on vowel harmony
- Uses both in_grouping_b_U and out_grouping_b_U to check for vowel presence and consonant context
- Critical for handling epenthetic vowel insertion in Turkish morphology
- Used specifically in possessive constructions where vowel insertion may be required
- Represents sophisticated morphophonological rules for maintaining Turkish syllable structure
- Part of the comprehensive vowel harmony and morphophonological system in PostgreSQL's Turkish Snowball stemmer

## Simplified Source

```c
static int r_mark_suffix_with_optional_U_vowel(struct SN_env * z) {
    int original_pos = z->l - z->c;

    // Case 1: Check if there's a high vowel 'U' (ı,i,u,ü) at current position
    if (in_grouping_b_U(z, g_U, 105, 305, 0) == 0) {
        // Test if 'U' vowel is preceded by a consonant
        int test_pos = z->l - z->c;
        if (out_grouping_b_U(z, g_vowel, 97, 305, 0) == 0) {
            // Valid: high vowel preceded by consonant (epenthetic vowel)
            z->c = z->l - test_pos;
            return 1;
        }
        // Restore position if consonant test failed
        z->c = z->l - original_pos;
    }

    // Case 2: No high vowel at current position - check constraints

    // First, ensure there's no high vowel that would create vowel cluster
    int check_pos = z->l - z->c;
    int test_pos = z->l - z->c;
    if (in_grouping_b_U(z, g_U, 105, 305, 0) == 0) {
        z->c = z->l - test_pos;
        return 0;  // Invalid: would create vowel cluster
    }
    z->c = z->l - check_pos;

    // Check that preceding character is a consonant
    test_pos = z->l - z->c;
    // Skip one UTF-8 character backward
    int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
    if (ret < 0) return 0;
    z->c = ret;

    if (out_grouping_b_U(z, g_vowel, 97, 305, 0) != 0)
        return 0;  // Invalid: not preceded by consonant

    z->c = z->l - test_pos;
    return 1;  // Valid pattern without optional vowel
}
```