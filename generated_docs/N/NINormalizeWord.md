# NINormalizeWord

## Location
src/backend/tsearch/spell.c: 2540 - 2606

## Overview
The main entry point for normalizing words using PostgreSQL's Ispell dictionary, producing all possible lexical forms including compound word variants.

## Definition
```c
TSLexeme *NINormalizeWord(IspellDict *Conf, char *word)
```

## Detailed Description
NINormalizeWord is the primary function for word normalization in PostgreSQL's Ispell-based text search dictionary. It processes input words through two main phases: first, it attempts direct normalization using NormalizeSubWord to find dictionary matches. Second, if compound word processing is enabled, it uses SplitToVariants to break the word into components and normalizes each part. The function returns a null-terminated array of TSLexeme structures containing all possible normalized forms with their associated variant numbers. This function is crucial for text search functionality as it bridges raw text input with the searchable lexical forms stored in the search index.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing dictionary rules and compound word settings
- `word`: Input word string to be normalized

## Dependencies
- Functions called/Symbols referenced:
  - NormalizeSubWord (direct word normalization)
  - SplitToVariants (compound word splitting)
  - addNorm (adding normalized forms to result array)
  - strlen (string length calculation)
  - pstrdup (string duplication)
  - palloc/pfree (memory management)
- Called from (representative examples):
  - dispell_lexize (at src/backend/tsearch/dict_ispell.c:125)

## Notes and Other Information
- Returns NULL if no normalization is possible
- Handles both simple words and compound words when usecompound is enabled
- Assigns incrementing variant numbers to distinguish different normalizations
- Manages memory allocation and cleanup for both successful and failed normalizations
- For compound words, processes all components and generates combinations of their normalized forms
- Critical component of PostgreSQL's full-text search infrastructure
- Used by the Ispell dictionary type in text search configurations