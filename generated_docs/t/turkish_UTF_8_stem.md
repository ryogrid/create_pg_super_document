# turkish_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2067-2092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L2067-L2092)

## Overview
Main stemming function for Turkish text processing that applies the complete Turkish stemming algorithm to reduce words to their root forms.

## Definition
extern int turkish_UTF_8_stem(struct SN_env * z)

## Detailed Description
This function implements the complete Turkish stemming algorithm following the Snowball stemming methodology. It processes Turkish words through a systematic pipeline: first verifying the word has sufficient complexity (more than one syllable), then applying nominal and verbal suffix removal, followed by noun suffix processing, and finally performing post-processing cleanup. The function uses backward processing (from end to beginning) typical of agglutinative languages like Turkish, where suffixes are progressively stripped while maintaining grammatical validity.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word to be stemmed along with processing state, cursors, and working memory

## Dependencies
- Functions called/Symbols referenced:
  - [r_more_than_one_syllable_word](../r/r_more_than_one_syllable_word.md)
  - [r_stem_nominal_verb_suffixes](../r/r_stem_nominal_verb_suffixes.md)  
  - [r_stem_noun_suffixes](../r/r_stem_noun_suffixes.md)
  - [r_postlude](../r/r_postlude.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer or external interface)

## Notes and Other Information
The function follows a strict processing order: syllable validation → nominal/verb suffix removal → noun suffix removal → post-processing. It uses cursor manipulation (z->lb, z->c, z->l) to track processing positions and employs the I[0] flag to control whether noun suffix processing should occur. Returns 1 on successful stemming, 0 on failure, and negative values for errors.

## Simplified Source

```c
extern int turkish_UTF_8_stem(struct SN_env * z) {
    // Step 1: Check if word has multiple syllables
    int ret = r_more_than_one_syllable_word(z);
    if (ret <= 0) return ret;

    // Step 2: Set up processing boundaries
    z->lb = z->c;    // Left boundary
    z->c = z->l;     // Start from end of word

    // Step 3: Remove nominal/verb suffixes
    int saved_pos = z->l - z->c;
    ret = r_stem_nominal_verb_suffixes(z);
    if (ret < 0) return ret;
    z->c = z->l - saved_pos;

    // Step 4: Remove noun suffixes (if flag allows)
    if (!z->I[0]) return 0;  // Check processing flag

    saved_pos = z->l - z->c;
    ret = r_stem_noun_suffixes(z);
    if (ret < 0) return ret;
    z->c = z->l - saved_pos;

    // Step 5: Post-processing cleanup
    z->c = z->lb;  // Reset to left boundary
    ret = r_postlude(z);
    if (ret <= 0) return ret;

    return 1;  // Successful stemming
}
```

**Key Logic**: Implements the complete Turkish stemming pipeline: validates multi-syllabic words, systematically removes verb/nominal suffixes then noun suffixes, and applies final cleanup. The algorithm processes words backward (typical for agglutinative languages) while using cursor positions and flags to maintain state throughout the stemming process.