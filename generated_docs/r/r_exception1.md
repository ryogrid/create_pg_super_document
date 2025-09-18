# r_exception1

## Location
src/backend/snowball/libstemmer/stem_UTF_8_english.c: 878 - 945

## Overview
Handles the first set of exceptional English words that require special transformation rather than standard stemming rules, converting irregular forms to their proper base forms.

## Definition
```c
static int r_exception1(struct SN_env * z)
```

## Detailed Description
This function implements Exception List 1 for the English Porter stemming algorithm. It identifies 18 specific irregular English words and transforms them to their correct base forms rather than applying standard stemming rules. The function performs forward pattern matching from the beginning of the word and only processes complete words (cursor must be at end of word after matching).

The exceptional words and their transformations include:
- "andes" → "ski", "atlas" → "sky" (unchanged), "bias" (unchanged), "cosmos" (unchanged)
- "dying" → "die", "early" → "earli", "gently" → "gentl", "howe" (unchanged)  
- "idly" → "idl", "lying" → "lie", "news" (unchanged), "only" → "onli"
- "singly" → "singl", "skies" → "ski", "skis" → "ski", "sky" (unchanged)
- "tying" → "tie", "ugly" → "ugli"

This handling is essential because these words would be incorrectly processed by normal morphological rules.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the Snowball environment with the word being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - find_among: Searches forward through the word for matching patterns from the a_10 array
  - slice_from_s: Replaces the matched word with the specified replacement string
  - a_10: Array of 18 exceptional word patterns with corresponding action codes
  - s_27 through s_37: Replacement strings for the transformed word forms
- Called from (representative examples):
  - english_ISO_8859_1_stem: Main English stemming function (called early to catch exceptions)
  - english_UTF_8_stem: UTF-8 version of English stemming

## Notes and Other Information
- This is part of the exception handling system, called before standard stemming rules
- Uses forward matching (find_among) rather than backward matching unlike most stemming steps
- Only processes complete words to avoid false matches within longer words
- Returns 1 if an exception is found and handled, 0 otherwise
- The transformations preserve semantic meaning while normalizing irregular morphological variations
- Essential for accuracy with English words that have irregular plural forms or other non-standard morphology
- Works in conjunction with r_exception2 to provide comprehensive exception handling