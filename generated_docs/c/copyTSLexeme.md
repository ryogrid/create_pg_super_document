# copyTSLexeme

## Location
src/backend/tsearch/dict_thesaurus.c: 753 - 770

## Overview
Creates a deep copy of a TSLexeme array from a TheSubstitute structure, duplicating both the array and all contained lexeme strings.

## Definition


## Detailed Description
The  function creates a complete deep copy of a  array stored within a  structure. It allocates memory for a new array with space for all result lexemes plus a NULL terminator, then copies each  structure while also duplicating the lexeme strings using . The function ensures that the copied array is properly null-terminated by setting the final element's lexeme field to NULL.

## Parameters / Member Variables
- : Pointer to the  structure containing the source  array to copy

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type containing the source TSLexeme array)
  -  (structure type for text search lexemes)
  -  (PostgreSQL memory allocation function)
  -  (PostgreSQL string duplication function)
- Called from:
  -  (at src/backend/tsearch/dict_thesaurus.c:780)

## Notes and Other Information
- Performs deep copying to ensure independence between original and copied lexeme arrays
- Allocates memory for  elements to include the NULL terminator
- Each lexeme string is individually duplicated to prevent shared memory references
- The resulting array is null-terminated following PostgreSQL TSLexeme array conventions
- This is a static function, only used internally within the thesaurus dictionary module
- Used in the thesaurus matching process to create independent copies of substitution results