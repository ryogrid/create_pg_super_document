# turkish_UTF_8_stem

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 2067 - 2092

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