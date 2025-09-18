# cmpTheLexeme

## Location
src/backend/tsearch/dict_thesaurus.c: 378 - 390

## Overview
A comprehensive comparison function for TheLexeme structures that compares both the lexeme strings and their associated LexemeInfo entries for complete ordering.

## Definition


## Detailed Description
This function provides a two-level comparison for TheLexeme structures, suitable for sorting operations that require both lexeme string ordering and LexemeInfo ordering. It first compares the lexeme strings using cmpLexeme, and if they are equal, it performs a secondary comparison using the LexemeInfo entries.

The function follows a hierarchical comparison strategy: primary sorting by lexeme string, secondary sorting by LexemeInfo properties. The negative sign applied to cmpLexemeInfo result suggests a reverse ordering preference for the LexemeInfo comparison, which may be used to prioritize certain lexeme entries over others when lexeme strings are identical.

## Parameters / Member Variables
- `a`: Void pointer to the first TheLexeme structure (cast from const void*)
- `b`: Void pointer to the second TheLexeme structure (cast from const void*)

## Dependencies
- Functions called/Symbols referenced:
  - [cmpLexeme](cmpLexeme.md) (primary comparison by lexeme string)
  - [cmpLexemeInfo](cmpLexemeInfo.md) (secondary comparison by LexemeInfo properties)
  - [TheLexeme](../T/TheLexeme.md) (structure type for casting)
- Called from (representative examples):
  - [compileTheLexeme](compileTheLexeme.md)

## Notes and Other Information
- Uses qsort-compatible function signature with void pointers
- Implements two-level sorting: first by lexeme string, then by LexemeInfo properties
- The negative sign on cmpLexemeInfo result indicates reverse ordering for secondary comparison
- Used during thesaurus compilation to maintain proper ordering of lexeme entries
- Essential for ensuring consistent and predictable ordering when multiple entries share the same lexeme string but differ in their associated metadata
- This comprehensive comparison enables efficient searching and prevents duplicate entries during thesaurus dictionary construction