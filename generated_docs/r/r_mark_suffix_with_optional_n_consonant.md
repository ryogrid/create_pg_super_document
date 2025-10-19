# r_mark_suffix_with_optional_n_consonant

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:512-545](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L512-L545)

## Overview
This function handles the morphological analysis of Turkish suffixes that may optionally include an 'n' consonant, implementing consonant insertion rules for Turkish stemming.

## Definition

```c
}

static int r_mark_suffix_with_optional_n_consonant(struct SN_env * z)
```
## Detailed Description
The  function is designed to handle Turkish morphological patterns where the consonant 'n' may be optionally inserted in certain suffix contexts. This is a common phenomenon in Turkish morphology where consonants are inserted to avoid vowel clusters or to maintain phonological well-formedness.

The function implements a two-stage checking mechanism:
1. First, it checks if there is an 'n' at the current position preceded by a vowel - this represents the case where the optional 'n' is present
2. If no 'n' is found, it validates that the current position follows the phonological constraints for cases where the 'n' is absent - specifically ensuring that there isn't already an 'n' that would create a double consonant, and that the preceding character is a vowel

This function is crucial for correctly identifying and processing Turkish suffixes that exhibit this optional consonant insertion pattern, which affects the morphological segmentation and analysis of Turkish words.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the text buffer, current position, and morphological analysis state
## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b_U](../i/in_grouping_b_U.md) (called 2 times for vowel group checking)
  - [skip_b_utf8](../s/skip_b_utf8.md) (for UTF-8 character boundary handling)
  - g_vowel (vowel group definition)

- Called from (representative examples):
  - [r_mark_nUn](r_mark_nUn.md) (suffix marking function)
  - [r_mark_ncA](r_mark_ncA.md) (suffix marking function)

## Notes and Other Information
- Returns 1 if the optional 'n' consonant pattern is valid, 0 otherwise
- The function uses backward scanning which is typical for suffix analysis in agglutinative languages
- The UTF-8 character handling indicates support for Turkish-specific characters
- This function represents a specialized morphophonological rule in Turkish grammar
- Used specifically by functions that handle suffixes containing optional 'n' consonants
- Part of the sophisticated Turkish morphological analysis system within PostgreSQL's Snowball stemmer

## Simplified Source

```c
static int r_mark_suffix_with_optional_n_consonant(struct SN_env * z) {
    int original_pos = z->l - z->c;

    // Case 1: Check if there's an 'n' at current position
    if (z->c > z->lb && z->p[z->c - 1] == 'n') {
        z->c--;
        // Test if 'n' is preceded by a vowel
        int test_pos = z->l - z->c;
        if (in_grouping_b_U(z, g_vowel, 97, 305, 0) == 0) {
            // Valid: 'n' preceded by vowel
            z->c = z->l - test_pos;
            return 1;
        }
        // Restore position if vowel test failed
        z->c = z->l - original_pos;
    }

    // Case 2: No 'n' at current position - check constraints

    // First, ensure there's no 'n' that would create double consonant
    int check_pos = z->l - z->c;
    int test_pos = z->l - z->c;
    if (z->c > z->lb && z->p[z->c - 1] == 'n') {
        z->c--;
        z->c = z->l - test_pos;
        return 0;  // Invalid: would create double 'n'
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
    return 1;  // Valid pattern without optional 'n'
}
```