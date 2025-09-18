# tlist_member

## Location
[src/backend/optimizer/util/tlist.c:79-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L79-L101)

## Overview
Searches a target list for an entry whose expression matches a given expression using deep equality comparison.

## Definition


## Detailed Description
The  function performs a linear search through a target list to find the first  whose expression is equal to the provided node expression. It uses PostgreSQL's  function to perform deep structural comparison of the expressions. This is a fundamental utility function used throughout the optimizer for finding existing target list entries to avoid duplication.

## Parameters / Member Variables
- : The expression to search for in the target list
- : A List of TargetEntry nodes to search through

## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md) (for deep expression comparison)
- Called from (representative examples):
  - [create_unique_plan](../c/create_unique_plan.md)
  - [search_indexed_tlist_for_non_var](../s/search_indexed_tlist_for_non_var.md)  
  - [preprocess_targetlist](../p/preprocess_targetlist.md)
  - [add_to_flat_tlist](../a/add_to_flat_tlist.md)
  - [apply_pathtarget_labeling_to_tlist](../a/apply_pathtarget_labeling_to_tlist.md)

## Notes and Other Information
- Returns the first matching TargetEntry or NULL if no match is found
- Uses linear search, so performance degrades with large target lists
- Critical for target list management and avoiding duplicate expressions
- Part of the target list utilities in the optimizer subsystem