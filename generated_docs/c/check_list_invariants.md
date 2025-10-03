# check_list_invariants

## Location
[src/backend/nodes/list.c:65-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L65-L80)

## Overview
A static validation function that verifies the integrity and consistency of PostgreSQL's List data structure by checking fundamental invariants.

## Definition

```c
static void
check_list_invariants(const List *list)
```
## Detailed Description
This function performs sanity checks on a List structure to ensure it maintains valid state. It validates that the list's internal properties are consistent and that the list type is one of the supported variants. The function is designed as a debugging aid and is called throughout the list manipulation functions to catch corruption early. It safely handles NIL lists and performs no action for them.

## Parameters / Member Variables
- `*list`: A pointer to the List structure to be validated. Can be NIL, in which case the function returns immediately without performing any checks.
## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for runtime assertions)
  - NIL (constant representing null list)
  - T_List, T_IntList, T_OidList, T_XidList (node type constants)
  
- Called from (representative examples):
  - [list_make1_impl](../l/list_make1_impl.md)
  - [list_make2_impl](../l/list_make2_impl.md)
  - [lappend](../l/lappend.md)
  - [list_insert_nth](../l/list_insert_nth.md)
  - [list_concat](../l/list_concat.md)
  - [list_member](../l/list_member.md)
  - [list_delete_nth_cell](../l/list_delete_nth_cell.md)

## Notes and Other Information
- This is a static function internal to list.c, not exposed in the public API
- The function validates four key invariants:
  1. Non-zero length for non-NIL lists
  2. Length does not exceed maximum capacity
  3. Elements array is allocated (not NULL)
  4. List type is one of the four supported types
- Used extensively throughout list operations as a defensive programming measure
- Compiled only in debug builds where assertions are enabled

## Simplified Source

```c
// Simplified version of check_list_invariants
static void check_list_invariants(const List *list) {
    // Skip validation for NULL lists
    if (list == NIL)
        return;

    // Core validation checks
    Assert(list->length > 0);                    // Non-empty list must have positive length
    Assert(list->length <= list->max_length);    // Length cannot exceed capacity
    Assert(list->elements != NULL);              // Elements array must be allocated

    // Validate list type is supported
    Assert(list->type == T_List ||               // Generic pointer list
           list->type == T_IntList ||            // Integer list
           list->type == T_OidList ||            // Object ID list
           list->type == T_XidList);             // Transaction ID list
}
```

Key simplifications made:
- Added descriptive comments for each validation check
- Grouped related assertions logically
- Clarified the purpose of each list type variant
- Preserved all original validation logic
- Maintained the early return for NIL lists