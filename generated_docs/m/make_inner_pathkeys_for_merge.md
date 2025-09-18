# make_inner_pathkeys_for_merge

## Location
src/backend/optimizer/path/pathkeys.c: 1835 - 1937

## Overview
This function builds a pathkey list representing the explicit sort order that must be applied to an inner path to make it usable with given mergeclauses.

## Definition


## Detailed Description
The function constructs the required pathkeys for the inner relation in a merge join by analyzing the mergeclauses and their relationship to the outer pathkeys. The algorithm processes mergeclauses in order and creates corresponding pathkeys for the inner side:

1. **Equivalence Class Extraction**: For each mergeclause, identifies the inner-side equivalence class
2. **Outer Pathkey Synchronization**: Ensures the outer equivalence class matches the expected outer pathkey sequence
3. **Inner Pathkey Creation**: Creates canonical pathkeys for the inner side, reusing outer pathkeys when equivalence classes match
4. **Redundancy Elimination**: Removes duplicate pathkeys to maintain canonical ordering

Key behaviors:
- Maintains synchronization between mergeclauses and outer_pathkeys
- Optimizes by reusing outer pathkeys when both sides have the same equivalence class
- Eliminates redundant pathkeys to ensure canonical sort key lists
- Includes comprehensive error checking for pathkey/mergeclause alignment

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : List of RestrictInfos for mergejoin clauses in order, marked with outer_is_left indicators
- : Already-known canonical pathkeys for the outer side of the join

## Dependencies
- Functions called/Symbols referenced:
  - [update_mergeclause_eclasses](../u/update_mergeclause_eclasses.md)
  - list_head
  - [lnext](../l/lnext.md)
  - [make_canonical_pathkey](make_canonical_pathkey.md)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md)
  - EquivalenceClass
  - PathKey
- Called from (representative examples):
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md) (src/backend/optimizer/path/joinpath.c:1409)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md) (src/backend/optimizer/path/joinpath.c:1519)

## Notes and Other Information
- Assumes sorting is necessary and focuses only on creating the correct pathkey ordering
- The restrictinfos must be pre-marked via outer_is_left to indicate which side is associated with the outer path
- Includes error checking to ensure outer pathkeys match the mergeclause sequence
- Output pathkey list may not be ordered exactly like mergeclauses due to redundancy elimination
- Complexity in create_mergejoin_plan() is introduced due to potential reordering from redundancy elimination
- Reuses outer pathkeys when inner and outer equivalence classes are identical for efficiency