# list_deduplicate_oid

## Location
src/backend/nodes/list.c: 1495 - 1519

## Overview
Removes adjacent duplicates from a sorted list of OIDs, modifying the list in-place to contain only unique values.

## Definition


## Detailed Description
This function efficiently removes duplicate OID values from a list by comparing adjacent elements. It assumes the caller has already sorted the list to bring duplicate values together, typically using `list_sort(list, list_oid_cmp)`. The function uses an in-place algorithm that maintains two pointers: one (i) tracks the position for writing unique values, while another (j) scans through all elements. When a new unique value is found, it's copied to the write position and the write pointer advances.

The algorithm runs in O(n) time complexity, making it efficient for large lists. After removing duplicates, the list's length is updated to reflect the new size, and list invariants are checked to ensure data structure integrity.

## Parameters / Member Variables
- `list`: The OID list to deduplicate. Must be sorted beforehand for the algorithm to work correctly. The list is modified in-place.

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList
  - list_length
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)
  - [GetPublicationRelations](../G/GetPublicationRelations.md)

## Notes and Other Information
- The caller must sort the list before calling this function - duplicates must be adjacent for removal
- The function modifies the list in-place without reallocating memory, only adjusting the length field
- Commonly used in conjunction with `list_sort(list, list_oid_cmp)` to create a sorted, unique list
- Time complexity is O(n) where n is the length of the list
- Used in foreign key constraint processing and publication relation management
- Part of PostgreSQL's generic list manipulation utilities in src/backend/nodes/list.c