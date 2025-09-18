# multirange_adjacent_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 2534 - 2574

## Overview
Determines if two multiranges are adjacent by checking if any boundary of one multirange is adjacent to any boundary of the other multirange.

## Definition


## Detailed Description
This function implements the SQL adjacency operator () for checking if two multiranges are adjacent. Two multiranges are considered adjacent if they share a boundary point without overlapping. The function performs a comprehensive check by examining the boundaries between the ranges in both multiranges.

The adjacency check works by:
1. Extracting both multirange arguments and performing empty checks
2. Getting the type cache for multirange operations
3. Checking if the last range of the first multirange is adjacent to the first range of the second multirange
4. If the first check fails and either multirange has multiple ranges, checking if the first range of the first multirange is adjacent to the last range of the second multirange
5. The function uses  to test for adjacency between range boundaries

The algorithm optimizes for the most common cases by checking the most likely adjacency scenarios first.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0:  - the first multirange to test for adjacency
  - Argument 1:  - the second multirange to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - extract multirange arguments
  -  - check if multiranges are empty
  -  - get type cache for multirange operations
  -  - get OID of multirange type
  -  - extract bounds from specific ranges within multiranges
  -  - [test](../t/test.md) if two range bounds are adjacent
- Called from (representative examples):
  - SQL queries using the  operator between two multiranges
  - PostgreSQL operator system via function catalog entries

## Notes and Other Information
- Returns a boolean result wrapped as a Datum using 
- Includes early empty check optimization that immediately returns false for empty operands
- Uses local variables to store range bounds ()
- The function checks multiple adjacency scenarios to handle complex multirange structures
- More complex than the range-multirange adjacency functions due to the need to check multiple boundary combinations
- Part of PostgreSQL's range and multirange type system for advanced range operations
- File location: 