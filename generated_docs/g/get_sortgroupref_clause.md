# get_sortgroupref_clause

## Location
src/backend/optimizer/util/tlist.c: 422 - 442

## Overview
Finds the SortGroupClause matching the given SortGroupRef index and returns it.

## Definition
SortGroupClause *get_sortgroupref_clause(Index sortref, List *clauses)

## Detailed Description
This function searches through a list of SortGroupClause structures to find the one whose tleSortGroupRef field matches the provided sortref index. It performs a linear search through the list, comparing each clause's tleSortGroupRef against the target index. If no matching clause is found, the function raises an ERROR with the message "ORDER/GROUP BY expression not found in list". This function is essential for looking up specific sort/group clauses when only the reference index is known.

## Parameters / Member Variables
- `sortref`: Index value (SortGroupRef) to search for in the clauses
- `clauses`: List of SortGroupClause structures to search within

## Dependencies
- Functions called/Symbols referenced:
  - SortGroupClause (structure type)
  - elog (error logging function)
  - Index (type alias)
- Called from (representative examples):
  - preprocess_groupclause

## Notes and Other Information
This function enforces the invariant that all referenced sort/group clauses must be present in the provided list by raising an error if a match is not found. This helps catch programming errors during query planning where references become inconsistent. The function is part of a family of utilities for working with sort and group clause lists, though it's less frequently used than its related functions. The linear search approach is acceptable given that sort/group clause lists are typically small.