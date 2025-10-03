# r_mark_sU

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:655-665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L655-L665)

## Overview
A static function in the Turkish stemmer that identifies and marks suffixes containing the Turkish vowel 'U' with proper vowel harmony checking and optional 's' consonant handling.

## Definition

```c
}

static int r_mark_sU(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish language stemming implementation that handles suffixes containing the vowel 'U' (which represents both 'u' and 'ü' in Turkish vowel harmony). The function first performs vowel harmony checking to ensure the suffix is compatible with the word stem according to Turkish phonological rules. It then checks if the current character belongs to the U vowel group (105, 305 representing 'ı' and 'İ' in UTF-8). Finally, it marks the suffix while handling optional 's' consonant variations that are common in Turkish morphology.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed, current position markers, and other stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md) (validates Turkish vowel harmony rules)
  - [in_grouping_b_U](../i/in_grouping_b_U.md) (checks if character belongs to U vowel group, backward direction)
  - [r_mark_suffix_with_optional_s_consonant](r_mark_suffix_with_optional_s_consonant.md) (handles optional 's' consonant in suffixes)
  - g_U (vowel group definition for U vowels)
- Called from (representative examples):
  - [r_stem_suffix_chain_before_ki](r_stem_suffix_chain_before_ki.md)
  - [r_stem_noun_suffixes](r_stem_noun_suffixes.md)

## Notes and Other Information
- Returns 1 on successful suffix identification and marking, 0 on failure
- The character range 105-305 corresponds to specific Turkish characters ('ı' and 'İ') in UTF-8 encoding
- Vowel harmony is a fundamental feature of Turkish where vowels in suffixes must harmonize with stem vowels
- The function operates in backwards mode for efficient suffix processing
- Part of the comprehensive Turkish morphological analysis system in PostgreSQL's full-text search