# r_remove_question_prefixes

## Location
src/backend/snowball/libstemmer/stem_UTF_8_tamil.c: 752 - 769

## Overview
A Tamil stemmer function that identifies and removes specific question-forming prefixes from Tamil words and applies character normalization after removal.

## Definition


## Detailed Description
This function is part of the Tamil language stemming algorithm that handles question-forming prefixes. It follows a specific pattern-matching sequence to identify Tamil question prefixes:

1. First checks for a 3-character pattern (s_12) at the current position
2. Then uses  to match against an array of 10 possible patterns (a_0)
3. Finally verifies another 3-character pattern (s_13)

If all three conditions are met, it removes the matched prefix using  and then applies character normalization by calling  to handle any character sequence adjustments that may be needed after prefix removal.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the current word being processed, cursor position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [eq_s](../e/eq_s.md) (string equality comparison, used twice for pattern matching)
  - [find_among](../f/find_among.md) (pattern matching function against an array of alternatives)
  - [slice_del](../s/slice_del.md) (function to delete the matched text segment)
  - [r_fix_va_start](r_fix_va_start.md) (character sequence normalization function)
- Called from (representative examples):
  - [tamil_UTF_8_stem](../t/tamil_UTF_8_stem.md) (main Tamil stemming function)

## Notes and Other Information
- Returns 1 on successful prefix removal, 0 if no matching pattern is found
- This is a static function with internal linkage, accessible only within the Tamil stemmer compilation unit
- Uses the bra/ket mechanism to mark the boundaries of text to be deleted
- The function employs cursor position management to ensure  doesn't affect the main processing position
- The use of  suggests this handles multiple variants of question-forming prefixes in Tamil