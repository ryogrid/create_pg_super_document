# NormalizeSubWord

## Location
src/backend/tsearch/spell.c: 2176 - 2284

## Overview
Generates all possible normalized forms of a word by systematically applying prefix and suffix transformations from the affix trees.

## Definition
```c
static char **NormalizeSubWord(IspellDict *Conf, char *word, int flag)
```

## Detailed Description
NormalizeSubWord is the core function for word normalization in PostgreSQL's spell checking system. It systematically explores all possible combinations of prefixes and suffixes to generate valid normalized forms of an input word. The function operates in three main phases:

1. **Base word check**: First checks if the word itself is already in normal form
2. **Prefix-only processing**: Tries all possible prefixes to find valid base forms
3. **Suffix-then-prefix processing**: Tries suffixes first, then applies prefixes to the suffix-transformed words

The function handles cross-product affixes (combinations of prefix and suffix) and validates each transformation against the dictionary. It maintains an array of unique results and prevents duplicates through the addToResult helper function.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing affix trees and dictionary
- `word`: Input word to normalize
- `flag`: Compound word flags indicating the word's position context

## Dependencies
- Functions called/Symbols referenced:
  - [FindAffixes](../F/FindAffixes.md) (called 3 times for prefix/suffix tree traversal)
  - [CheckAffix](../C/CheckAffix.md) (called 3 times for affix validation and transformation)
  - [addToResult](../a/addToResult.md) (called 3 times for result collection)
  - [FindWord](../F/FindWord.md) (called 4 times for dictionary validation)
  - strlen, pstrdup, palloc, pfree (utility functions)
- Called from (representative examples):
  - SplitToVariants (at line 2427)
  - NINormalizeWord (at lines 2547, 2572)

## Notes and Other Information
- Returns NULL if no valid normalized forms are found or if word exceeds MAXNORMLEN (256 characters)
- Allocates memory for up to MAX_NORM (1024) result forms
- Handles FF_CROSSPRODUCT flag for valid prefix-suffix combinations
- Uses separate buffers (newword, pnewword) for different transformation stages
- Part of PostgreSQL's text search spell checking functionality
- The baselen parameter tracks word boundaries when processing compound affixes