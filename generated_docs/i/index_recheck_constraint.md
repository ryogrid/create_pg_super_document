# index_recheck_constraint

## Location
src/backend/executor/execIndexing.c: 932 - 962

## Overview
A static function that validates whether an existing tuple's indexed values truly conflict with new values according to exclusion constraint operators, returning true if a conflict exists.

## Definition


## Detailed Description
This function performs a detailed comparison between an existing tuple's indexed values and new values to determine if they genuinely violate an exclusion constraint. It iterates through all key attributes of the index, applying the appropriate exclusion operators to compare corresponding values. The function assumes that exclusion operators are strict (return NULL when any operand is NULL), and treats NULL values as non-conflicting.

For each key attribute, the function calls the exclusion operator using the index's collation settings. If any operator returns false, indicating no conflict for that attribute, the entire constraint check returns false. Only when all operators return true does the function conclude that a genuine conflict exists.

## Parameters / Member Variables
- : The index relation containing the exclusion constraint definition
- : Array of OIDs representing the exclusion operators for each key attribute
- : Array of Datum values from the existing tuple being compared
- : Array of boolean flags indicating which existing values are NULL
- : Array of Datum values from the new tuple being checked

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - OidFunctionCall2Coll
- Called from (representative examples):
  - check_exclusion_or_unique_constraint

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execIndexing.c file
- The function assumes exclusion operators are strict, meaning they return NULL (treated as false) when any operand is NULL
- Uses the index's collation settings when calling comparison operators via OidFunctionCall2Coll
- Part of PostgreSQL's exclusion constraint enforcement mechanism, specifically handling the detailed value comparison logic
- The function performs short-circuit evaluation - if any attribute comparison returns false, the entire constraint check terminates early