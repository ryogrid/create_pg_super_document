# append_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 106 - 157

## Overview
Appends all non-redundant PathKeys from a source list to a target list, ensuring no duplicate ordering specifications are added.

## Definition


## Detailed Description
This function efficiently merges two lists of PathKeys by appending only the non-redundant PathKeys from the source list to the target list. It uses the  function to check each PathKey in the source list against the existing PathKeys in the target list, preventing the addition of duplicate or redundant ordering specifications.

The function is essential for combining ordering requirements from different parts of a query plan while maintaining efficiency by avoiding redundant sort specifications. This is particularly important during query optimization when multiple ordering constraints need to be consolidated.

## Parameters / Member Variables
- : The destination list of PathKeys that will be extended (must not be NIL)
- : The source list of PathKeys to be appended to the target

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - lfirst_node (list iteration with node type checking)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md) (redundancy checking function)
  - lappend (list append operation)
- Called from (representative examples):
  - [adjust_group_pathkeys_for_groupagg](adjust_group_pathkeys_for_groupagg.md)
  - [make_pathkeys_for_window](../m/make_pathkeys_for_window.md)

## Notes and Other Information
- The target list must not be NIL (assertion enforced)
- Returns the updated target list
- Only adds PathKeys that are not redundant with respect to the existing target list
- Used in query planning to combine ordering requirements from different operations
- Located in src/backend/optimizer/path/pathkeys.c:106-157