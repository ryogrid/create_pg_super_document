# r_mark_suffix_with_optional_s_consonant

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:546-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L546-L579)

## Overview
This function handles the morphological analysis of Turkish suffixes that may optionally include an 's' consonant, implementing consonant insertion rules for Turkish stemming.

## Definition

```c
}

static int r_mark_suffix_with_optional_s_consonant(struct SN_env * z)
```
## Detailed Description
The  function is designed to handle Turkish morphological patterns where the consonant 's' may be optionally inserted in certain suffix contexts. This function mirrors the behavior of  but specifically handles the 's' consonant insertion pattern.

The function implements the same two-stage checking mechanism as its 'n' counterpart:
1. First, it checks if there is an 's' at the current position preceded by a vowel - representing cases where the optional 's' is present
2. If no 's' is found, it validates the phonological constraints for cases where the 's' is absent - ensuring no double 's' consonants and that the preceding character is a vowel

This type of consonant insertion is part of Turkish morphophonological processes that maintain phonological well-formedness and avoid problematic sound sequences. The 's' insertion particularly occurs in certain possessive and other grammatical constructions in Turkish.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the text buffer, current position, and morphological analysis state
## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b_U](../i/in_grouping_b_U.md) (called 2 times for vowel group checking)
  - [skip_b_utf8](../s/skip_b_utf8.md) (for UTF-8 character boundary handling)
  - g_vowel (vowel group definition)

- Called from (representative examples):
  - [r_mark_sU](r_mark_sU.md) (suffix marking function for possessive 's' + vowel patterns)

## Notes and Other Information
- Returns 1 if the optional 's' consonant pattern is valid, 0 otherwise
- Structurally identical to  but handles 's' instead of 'n'
- Uses backward scanning for suffix analysis typical in agglutinative language processing
- The UTF-8 support ensures proper handling of Turkish-specific characters
- Represents specialized morphophonological rules specific to Turkish 's' insertion
- Used primarily for possessive and similar constructions where 's' may be optionally inserted
- Part of the comprehensive Turkish morphological analysis within PostgreSQL's Snowball stemmer

## Simplified Source

```c
static int r_mark_suffix_with_optional_s_consonant(struct SN_env * z) {
    int original_pos = z->l - z->c;

    // Case 1: Check if there's an 's' at current position
    if (z->c > z->lb && z->p[z->c - 1] == 's') {
        z->c--;
        // Test if 's' is preceded by a vowel
        int test_pos = z->l - z->c;
        if (in_grouping_b_U(z, g_vowel, 97, 305, 0) == 0) {
            // Valid: 's' preceded by vowel
            z->c = z->l - test_pos;
            return 1;
        }
        // Restore position if vowel test failed
        z->c = z->l - original_pos;
    }

    // Case 2: No 's' at current position - check constraints

    // First, ensure there's no 's' that would create double consonant
    int check_pos = z->l - z->c;
    int test_pos = z->l - z->c;
    if (z->c > z->lb && z->p[z->c - 1] == 's') {
        z->c--;
        z->c = z->l - test_pos;
        return 0;  // Invalid: would create double 's'
    }
    z->c = z->l - check_pos;

    // Check that preceding character is a vowel
    test_pos = z->l - z->c;
    // Skip one UTF-8 character backward
    int ret = skip_b_utf8(z->p, z->c, z->lb, 1);
    if (ret < 0) return 0;
    z->c = ret;

    if (in_grouping_b_U(z, g_vowel, 97, 305, 0) != 0)
        return 0;  // Invalid: not preceded by vowel

    z->c = z->l - test_pos;
    return 1;  // Valid pattern without optional 's'
}
```