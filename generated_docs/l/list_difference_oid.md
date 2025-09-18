# list_difference_oid

## Location
src/backend/nodes/list.c: 1313 - 1342

## Overview
Returns a new list containing OID elements from the first list that are not present in the second list, using OID value equality for membership comparison.

## Definition
```c
List *list_difference_oid(const List *list1, const List *list2)
```

## Detailed Description
This function creates a new list containing all OID (Object Identifier) elements from `list1` that are not found in `list2`. It is a specialized variant of `list_difference()` that operates specifically on lists of OIDs, which are unsigned integer identifiers used throughout PostgreSQL to uniquely identify database objects.

The function iterates through each OID element in `list1` and checks if that OID value exists in `list2`. If not found, the OID is appended to the result list. The original lists remain unchanged. This variant is optimized for OID lists and uses the appropriate OID-specific list operations.

## Parameters / Member Variables
- `list1`: The source list of OIDs from which elements will be selected
- `list2`: The list of OIDs containing elements to be excluded from the result (can be NIL)

## Dependencies
- Functions called/Symbols referenced:
  - `IsOidList` - Asserts that both input lists are OID lists
  - [list_copy](list_copy.md) - Creates a copy of list1 when list2 is NIL
  - [list_member_oid](list_member_oid.md) - Checks OID membership in list2
  - `lfirst_oid` - Extracts OID values from list cells
  - `lappend_oid` - Appends OID elements to the result list
  - [check_list_invariants](../c/check_list_invariants.md) - Validates the final result list
- Called from (representative examples):
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md) (src/backend/commands/publicationcmds.c:1309)

## Notes and Other Information
- Both input lists must be OID lists (verified by assertions)
- Returns a copy of list1 if list2 is NIL (empty)
- Uses OID value equality for membership testing
- The result list maintains the original order of elements from list1
- Uses OID-specific list functions for better performance with OID data
- OIDs are PostgreSQL's standard way of identifying database objects like tables, functions, types, etc.
- Memory for the result list is newly allocated and should be freed when no longer needed