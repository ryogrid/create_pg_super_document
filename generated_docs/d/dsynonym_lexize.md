# dsynonym_lexize

## Location
src/backend/tsearch/dict_synonym.c: 210 - 241

## Overview
Performs synonym replacement by searching for input tokens in the initialized synonym dictionary and returning appropriate lexeme replacements.

## Definition


## Detailed Description
This function is the core lexicalization routine for PostgreSQL's synonym dictionary. It takes an input token and searches the pre-built sorted synonym array to find matching entries. When a match is found, it returns the corresponding synonym as a TSLexeme array.

The function performs these operations:
1. Extracts the input token and its length from function arguments
2. Creates a search key, applying case conversion if necessary
3. Performs binary search using bsearch() and compareSyn() for efficient lookup
4. Returns a TSLexeme array containing the synonym replacement if found
5. Returns NULL if no matching synonym exists

The function respects the case sensitivity setting established during dictionary initialization.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - d: Pointer to initialized DictSyn structure
  - in: Input token to look up
  - len: Length of input token

## Dependencies
- Functions called/Symbols referenced:
  - DictSyn, Syn, TSLexeme (data structures)
  - [pnstrdup](../p/pnstrdup.md) (string duplication with length)
  - [lowerstr_with_len](../l/lowerstr_with_len.md) (case conversion)
  - bsearch with compareSyn (binary search)
  - [palloc0](../p/palloc0.md) (memory allocation)
- Called from (representative examples):
  - PostgreSQL text search lexicalization system (no direct callers in provided data)

## Notes and Other Information
- This is a PostgreSQL function callable during text search processing
- Uses binary search for O(log n) lookup performance on large synonym dictionaries
- Handles case sensitivity by converting search keys to lowercase when appropriate
- Returns a TSLexeme array with exactly one synonym entry plus a NULL terminator
- Preserves prefix flags from the original synonym definition
- Protects against Solaris bsearch bug by checking array length before searching