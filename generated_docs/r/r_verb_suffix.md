# r_verb_suffix

## Location
src/backend/snowball/libstemmer/stem_UTF_8_spanish.c: 902 - 940

## Overview
This function removes verb-specific suffixes from words in the Snowball stemming algorithm, handling a large collection of verb endings with different region requirements.

## Definition
```c
static int r_verb_suffix(struct SN_env * z)
```

## Detailed Description
The `r_verb_suffix` function is a specialized component of the Snowball stemming algorithm dedicated to handling verb-specific suffixes. It processes an extensive collection of 283 verb suffix patterns (stored in array a_3) and categorizes them into two main processing groups:

1. **Category 1**: Verb suffixes that require R1 region validation before removal
2. **Category 2**: Verb suffixes that require R2 region validation before removal

The function uses backward pattern matching to identify verb suffixes from the end of words, then applies the appropriate removal rule based on the morphological complexity of the suffix. Category 2 suffixes (requiring R2 validation) are typically more complex or less common verb forms, while Category 1 suffixes (requiring only R1 validation) represent more basic verb inflections.

This function is essential for properly stemming verbs in Romance languages, where verb conjugation produces numerous suffix variations for tense, mood, person, and number.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including:
  - `c`: Current cursor position
  - `ket`: End position marker
  - `bra`: Start position marker
  - `p`: Pointer to the string being processed
- `among_var`: Local variable storing the category number (1-2) of the matched verb suffix pattern

## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md) (tests if position is within R1 region)
  - [r_R2](r_R2.md) (tests if position is within R2 region)
  - [find_among_b](../f/find_among_b.md) (backward pattern matching function)
  - [slice_del](../s/slice_del.md) (deletes text between bra and ket positions)
- Called from (representative examples):
  - [catalan_ISO_8859_1_stem](../c/catalan_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c:1418)
  - [french_ISO_8859_1_stem](../f/french_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_french.c:1184)
  - [italian_ISO_8859_1_stem](../i/italian_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_italian.c:994)
  - [portuguese_ISO_8859_1_stem](../p/portuguese_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_portuguese.c:902)
  - [spanish_ISO_8859_1_stem](../s/spanish_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_spanish.c:1013)

## Notes and Other Information
- The function handles 283 different verb suffix patterns, making it one of the most comprehensive verb-specific processing functions in the stemmer
- The two-category system reflects different levels of morphological complexity in verb suffixes
- Unlike other suffix functions, this one focuses exclusively on verbal morphology rather than mixed word classes
- The function is widely used across Romance language stemmers (Catalan, French, Italian, Portuguese, Spanish, Romanian)
- Returns 1 on successful removal, 0 if no verb suffix matched, and negative values on error
- All matched suffixes are completely removed rather than replaced, indicating that verb suffix removal aims to reach the verb root
- The large number of patterns (283) reflects the rich verbal morphology characteristic of Romance languages