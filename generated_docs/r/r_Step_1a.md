# r_Step_1a

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c: 481 - 536

## Overview
The r_Step_1a function implements Step 1a of the English Porter stemming algorithm in the Snowball stemmer, handling the removal of certain possessive and plural suffixes.

## Definition
```c
static int r_Step_1a(struct SN_env * z)
```

## Detailed Description
This function performs the first step of the English stemming algorithm, processing two types of suffix patterns:

1. **Possessive suffixes**: Removes apostrophes and possessive forms (' , 's, 's')
2. **Plural suffixes**: Handles various plural endings (ied, s, ies, sses, ss, us) with specific transformations

The function operates in two main phases:
- First phase: Attempts to match and remove possessive suffixes using the a_1 array
- Second phase: Matches plural suffixes using the a_2 array and applies appropriate transformations

For plural suffixes, it applies these rules:
- **Case 1** (sses): Transforms to "ss" 
- **Case 2** (ied/ies): Transforms to "i" if preceded by only one letter, otherwise to "ie"
- **Case 3** (s): Deletes the 's' only if preceded by a valid letter (not in vowel group) and followed by a vowel

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with:
  - : Current cursor position
  - : Length of the string
  - : Left boundary limit
  - : Character array being processed
  - : End marker for current suffix
  - : Start marker for current suffix

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (searches backwards for matching suffixes)
  - [slice_del](../s/slice_del.md) (deletes the marked substring)
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - [out_grouping_b](../o/out_grouping_b.md) (checks if character is outside specified group)
  - a_1 (array of possessive suffixes: ', 's', 's)
  - a_2 (array of plural suffixes: ied, s, ies, sses, ss, us)
  - s_2, s_3, s_4 (replacement strings: "ss", "i", "ie")
  - g_v (vowel character group for validation)
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- Uses backward searching (find_among_b) to match suffixes from the end of the word
- Implements sophisticated logic for 'ied/ies' handling based on word length
- The vowel check in case 3 ensures that 's' is only removed from valid plural forms
- Part of the standard Porter stemming algorithm implementation in PostgreSQL's full-text search capabilities