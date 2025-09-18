# check_list_invariants

## Location
src/backend/nodes/list.c: 65 - 80

## Overview
A static validation function that verifies the integrity and consistency of PostgreSQL's List data structure by checking fundamental invariants.

## Definition


## Detailed Description
This function performs sanity checks on a List structure to ensure it maintains valid state. It validates that the list's internal properties are consistent and that the list type is one of the supported variants. The function is designed as a debugging aid and is called throughout the list manipulation functions to catch corruption early. It safely handles NIL lists and performs no action for them.

## Parameters / Member Variables
- : A pointer to the List structure to be validated. Can be NIL, in which case the function returns immediately without performing any checks.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for runtime assertions)
  - NIL (constant representing null list)
  - T_List, T_IntList, T_OidList, T_XidList (node type constants)
  
- Called from (representative examples):
  - list_make1_impl
  - list_make2_impl
  - lappend
  - list_insert_nth
  - list_concat
  - list_member
  - list_delete_nth_cell

## Notes and Other Information
- This is a static function internal to list.c, not exposed in the public API
- The function validates four key invariants:
  1. Non-zero length for non-NIL lists
  2. Length does not exceed maximum capacity
  3. Elements array is allocated (not NULL)
  4. List type is one of the four supported types
- Used extensively throughout list operations as a defensive programming measure
- Compiled only in debug builds where assertions are enabled