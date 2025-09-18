# r_exception2

## Location
src/backend/snowball/libstemmer/stem_UTF_8_english.c: 869 - 877

## Overview  
Handles exceptional words that should not be processed by the standard stemming rules, specifically identifying and preserving certain complete words ending in "d" or "g".

## Definition
```c
static int r_exception2(struct SN_env * z)
```

## Detailed Description
This function implements Exception List 2 for the English Porter stemming algorithm. It identifies specific complete words that should not be modified by the normal stemming process. The function checks if the entire word matches one of eight predefined exceptions: "succeed", "proceed", "exceed", "canning", "inning", "earring", "herring", or "outing".

The function only matches complete words (cursor must be at the beginning of the word after finding a match) and performs a backwards search from the current position. This prevents partial matches within longer words and ensures that only these specific exceptional forms are preserved without modification.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the Snowball environment with the word being processed and cursor positions

## Dependencies  
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Searches backwards through the word for matching patterns from the a_9 array
  - a_9: Array of 8 exceptional word patterns that should not be stemmed
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md): Main English stemming function (called early to catch exceptions)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md): UTF-8 version of English stemming

## Notes and Other Information
- This is part of the exception handling system in the Porter stemming algorithm
- Only processes words ending in "d" or "g" as an optimization (checked via character range test)
- Returns 1 if an exception is found (indicating the word should not be further processed), 0 otherwise  
- These words are preserved because they would be incorrectly modified by normal stemming rules
- Called early in the stemming process before any standard morphological rules are applied
- Essential for maintaining accuracy with irregular English words that do not follow standard morphological patterns