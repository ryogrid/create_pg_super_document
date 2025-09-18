# r_fix_endings

## Location
src/backend/snowball/libstemmer/stem_UTF_8_tamil.c: 734 - 751

## Overview
A Tamil stemmer function that repeatedly applies ending fixes to a word by calling  in a loop until no more fixes can be applied.

## Definition


## Detailed Description
This function serves as a controller for the Tamil ending normalization process. It implements a loop that continuously calls  to perform character sequence corrections at word endings until no more transformations are possible. This iterative approach ensures that all applicable ending fixes are applied, even when one fix might enable another fix to be applied.

The function uses cursor position management to ensure that failed attempts don't affect the text position, restoring the cursor to its original position after the fixing process completes.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the current word being processed, cursor position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_fix_ending](r_fix_ending.md) (the actual ending fix function that performs individual transformations)
- Called from (representative examples):
  - [r_remove_question_suffixes](r_remove_question_suffixes.md) (Tamil question suffix removal function)
  - [r_remove_common_word_endings](r_remove_common_word_endings.md) (Tamil common word ending removal function)  
  - [r_remove_vetrumai_urupukal](r_remove_vetrumai_urupukal.md) (Tamil case marker removal function)
  - [r_remove_tense_suffix](r_remove_tense_suffix.md) (Tamil tense suffix removal function)

## Notes and Other Information
- Always returns 1 (success), indicating the function completes regardless of whether any fixes were applied
- This is a static function with internal linkage, accessible only within the Tamil stemmer compilation unit
- The loop continues until  returns 0, indicating no more fixes can be applied
- Uses cursor position management (c1, c2) to maintain state and handle backtracking
- This pattern of iterative application is common in stemming algorithms where multiple related transformations may be needed