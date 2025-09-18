# list_copy_head

## Location
src/backend/nodes/list.c: 1593 - 1612

## Overview
Creates a shallow copy of a PostgreSQL list containing only the first 'len' elements of the original list.

## Definition


## Detailed Description
The  function creates a shallow copy of the first portion of a PostgreSQL List structure. It copies only the first 'len' elements from the source list, or the entire list if it contains fewer than 'len' elements. Like , this performs a shallow copy operation where only the list structure and element pointers are duplicated.

The function includes several safety checks: it returns NIL if the input list is NIL or if len is zero or negative. It uses  to ensure that the requested length doesn't exceed the actual list length, preventing buffer overruns.

This function is particularly useful in query optimization and planning where only a subset of a list (typically the most significant elements) is needed for processing.

## Parameters / Member Variables
- : The source List from which to copy the head elements. Can be NIL.
- : The maximum number of elements to copy from the beginning of the list. If negative or zero, NIL is returned.

## Dependencies
- Functions called/Symbols referenced:
  - new_list
  - check_list_invariants
- Called from (representative examples):
  - get_object_address_relobject
  - expand_indexqual_rowcompare
  - group_keys_reorder_by_pathkeys
  - truncate_useless_pathkeys
  - create_append_plan
  - create_merge_append_plan

## Notes and Other Information
- This is a shallow copy operation - only the list structure is duplicated, not the data elements
- Safe to call with NIL input or non-positive len values
- Uses  macro to prevent copying beyond the list boundary  
- Commonly used in query planning to extract the most relevant portion of pathkey lists
- The copied list maintains the same type as the original
- Memory allocation is handled by the  function