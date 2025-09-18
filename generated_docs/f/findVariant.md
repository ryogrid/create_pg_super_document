# findVariant

## Location
src/backend/tsearch/dict_thesaurus.c: 696 - 752

## Overview
Finds matching lexeme variants by coordinating substitution IDs across multiple input lexeme arrays and building a linked list of compatible variants.

## Definition


## Detailed Description
The  function implements a complex algorithm to find lexeme variants that match specific criteria for thesaurus substitution. It processes arrays of  pointers () to find entries with matching substitution IDs, positions, and variant counts. The function coordinates across multiple lexeme lists to ensure all input words have compatible substitution patterns. It builds a linked list of matching variants by linking them through the  field and returns the head of this list.

## Parameters / Member Variables
- : Input linked list of lexeme variants to extend (may be NULL)
- : Previously stored lexeme information to validate against using 
- : Current position within the substitution pattern being processed
- : Array of pointers to  structures representing input lexeme lists
- : Number of elements in the  array

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for lexeme information and variant chaining)
  -  (validates substitution ID compatibility)
  -  (structure type referenced at end)
- Called from:
  -  (at src/backend/tsearch/dict_thesaurus.c:848)
  -  (at src/backend/tsearch/dict_thesaurus.c:855)

## Notes and Other Information
- Uses a complex nested loop structure to coordinate across multiple input lexeme arrays
- Advances through  chains in the  arrays to find matching substitution patterns
- Only adds variants that match position and variant count requirements ()
- Validates substitution compatibility using  for both stored and input lexemes
- This is a static function, only used internally within the thesaurus dictionary module
- The algorithm ensures that all input words participate in the same substitution rule