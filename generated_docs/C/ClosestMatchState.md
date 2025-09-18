# ClosestMatchState

## Location
src/include/utils/varlena.h: 41 - 47

## Overview
ClosestMatchState is a data structure used for finding the closest matching string from a set of candidates using Levenshtein distance calculation, primarily for providing helpful hints in error messages.

## Definition


## Detailed Description
The ClosestMatchState structure implements a stateful approach to finding the closest string match using the Levenshtein distance algorithm. It is designed to help PostgreSQL provide meaningful suggestions when users provide invalid or misspelled identifiers, such as column names, function names, or configuration options.

The structure maintains state during a search process where multiple candidate strings are evaluated against a source string. It tracks the best match found so far (lowest Levenshtein distance) within specified constraints. The algorithm enforces several practical limits: the maximum allowed distance must not exceed half the length of the source string, and the total string length must not exceed MAX_LEVENSHTEIN_STRLEN to avoid performance issues.

## Parameters / Member Variables
- : Pointer to the source string that needs to be matched against candidates
- : Minimum Levenshtein distance found so far (-1 indicates no valid match has been found yet)
- : Maximum Levenshtein distance allowed for a candidate to be considered a valid match
- : Pointer to the best matching candidate string found so far (NULL if no suitable match exists)

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with no direct function calls)
- Called from (representative examples):
  - [initClosestMatch](../i/initClosestMatch.md)
  - [updateClosestMatch](../u/updateClosestMatch.md)  
  - [getClosestMatch](../g/getClosestMatch.md)
  - [postgresql_fdw_validator](../p/postgresql_fdw_validator.md)

## Notes and Other Information
This structure is typically used in a three-phase pattern:
1. Initialize with  specifying the source string and maximum distance
2. Iterate through candidates using  to find the best match
3. Retrieve the result with 

The implementation is commonly used in PostgreSQL's error reporting system to suggest corrections for misspelled identifiers. For example, it's used in foreign data wrapper validation to suggest correct connection option names when invalid ones are provided. The Levenshtein distance constraint ensures that only reasonably similar strings are suggested, preventing confusing recommendations.