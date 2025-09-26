# check_weight

## Location
src/backend/utils/adt/tsvector_op.c: 2295 - 2309

## Overview
A static utility function that counts the number of position entries within a TSVector word entry that match a specified weight bitmask.

## Definition


## Detailed Description
The  function is a helper function used in PostgreSQL's text search statistics () functionality. It examines the position data for a specific word entry within a TSVector and counts how many positions have weights that match the provided weight bitmask. Each position in a TSVector can have an associated weight (A, B, C, or D), and this function checks if any of the positions match the weights specified in the bitmask parameter.

The function iterates through all position entries for the given word, extracts the weight of each position using , and checks if that weight bit is set in the provided weight bitmask. It returns the total count of matching positions.

## Parameters / Member Variables
- : The TSVector containing the word and position data
- : Pointer to the specific WordEntry within the TSVector to examine
- : An 8-bit bitmask specifying which weights to count (bits correspond to weight categories A, B, C, D)

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the length of position data for a word entry
  - : Macro to get a pointer to the position data for a word entry
  - : Macro to extract the weight from a WordEntryPos
  - : Structure representing a word entry in TSVector
  - : Structure representing position information for a word
  - : Text search vector type
- Called from (representative examples):
  - : Uses this function to count weighted positions when building statistics

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- Part of PostgreSQL's full-text search statistics support ()
- The weight parameter uses bit manipulation to allow checking multiple weight categories in a single call
- Weights in PostgreSQL text search correspond to importance levels (A=highest, D=lowest)
- Located in 
- The function is performance-critical as it's called for each word when building text search statistics