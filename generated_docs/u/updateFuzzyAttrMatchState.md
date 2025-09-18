# updateFuzzyAttrMatchState

## Location
[src/backend/parser/parse_relation.c:587-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L587-L679)

## Overview
Updates the fuzzy attribute matching state by comparing a candidate column name against a target using Levenshtein distance to find the best approximate matches.

## Definition


## Detailed Description
This static function implements fuzzy matching logic for PostgreSQL's column name resolution. When an exact column name match fails, this function evaluates potential column candidates using the Levenshtein distance algorithm to suggest similar column names. It maintains state about the best and second-best matches found so far, considering both RTE-level penalties and column name differences. The function employs several heuristics to avoid suggesting unreasonable matches, such as rejecting matches where more than half the characters differ, and handles cases with multiple equally-distant matches to avoid ambiguous suggestions.

## Parameters / Member Variables
- : Integer penalty value for RTE-level matching (helps distinguish between different table matches)
- : Pointer to FuzzyAttrMatchState structure that tracks the best matches found
- : RangeTblEntry pointer representing the table/relation being considered
- : String containing the actual column name being evaluated as a match candidate
- : String containing the target column name we're trying to match against
- : Integer attribute number of the column being considered

## Dependencies
- Functions called/Symbols referenced:
  - [FuzzyAttrMatchState](../F/FuzzyAttrMatchState.md) (structure type)
  - [varstr_levenshtein_less_equal](../v/varstr_levenshtein_less_equal.md) (Levenshtein distance calculation)
- Called from (representative examples):
  - [scanRTEForColumn](../s/scanRTEForColumn.md)

## Notes and Other Information
- Static function, only accessible within parse_relation.c
- Rejects dropped columns (identified by empty actual names)
- Uses configurable Levenshtein distance parameters (1,1,1 for insertion, deletion, substitution costs)
- Implements smart rejection of matches where >50% of characters differ
- Manages complex state tracking for first/best and second-best matches to handle tie-breaking
- Essential component of PostgreSQL's user-friendly error reporting for column name typos
- Located in src/backend/parser/parse_relation.c:587-679