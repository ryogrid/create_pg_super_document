# compare_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 302 - 340

## Overview
Compares two pathkey lists to determine if they are equivalent, and if not, which one is "better" in terms of sort ordering.

## Definition


## Detailed Description
The compare_pathkeys function performs a comparison between two lists of pathkeys to determine their relationship. It assumes that both pathkey lists are canonical, which allows for equality checking through simple pointer comparison. The function returns one of four possible comparison results: PATHKEYS_EQUAL (identical lists), PATHKEYS_DIFFERENT (incompatible orderings), PATHKEYS_BETTER1 (keys1 is a prefix extension of keys2), or PATHKEYS_BETTER2 (keys2 is a prefix extension of keys1).

The comparison algorithm first checks for identical list pointers as an optimization, then iterates through both lists simultaneously using the forboth macro. If any pathkey pair differs by pointer comparison, the lists are considered different. If one list is longer than the other but the shorter list matches as a prefix, the longer list is considered "better" as it provides additional sort ordering.

## Parameters / Member Variables
- : First pathkey list to compare
- : Second pathkey list to compare

## Dependencies
- Functions called/Symbols referenced:
  - forboth (macro for iterating two lists simultaneously)
  - PathKey (pathkey structure type)
- Return values:
  - PATHKEYS_EQUAL
  - PATHKEYS_DIFFERENT
  - PATHKEYS_BETTER1
  - PATHKEYS_BETTER2
- Called from (representative examples):
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md)
  - [set_cheapest](../s/set_cheapest.md)
  - [add_path](../a/add_path.md)

## Notes and Other Information
This function is fundamental to PostgreSQL's query optimization process, as it enables the planner to determine which paths provide better sort ordering. The canonical nature of pathkeys allows for efficient pointer-based equality checking rather than deep structural comparison.