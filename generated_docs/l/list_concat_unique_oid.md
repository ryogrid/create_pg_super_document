# list_concat_unique_oid

## Location
src/backend/nodes/list.c: 1469 - 1494

## Overview
Concatenates two lists of OIDs, ensuring that duplicates from the second list are not added to the first list.

## Definition


## Detailed Description
This function is a specialized variant of  that operates specifically on lists containing OID (Object Identifier) values. It iterates through each element in the second list and appends it to the first list only if that OID is not already present in the first list. This ensures the resulting list contains unique OIDs while preserving the original order of elements from both lists.

The function modifies and returns the first list parameter, making it the concatenated result. It includes assertions to verify that both input parameters are indeed OID lists and performs list invariant checking to maintain data structure integrity.

## Parameters / Member Variables
- `list1`: The target OID list that will be modified and returned. Elements from list2 will be appended to this list if they are unique.
- `list2`: The source OID list whose elements will be checked for uniqueness and potentially appended to list1. This parameter is const and remains unchanged.

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList
  - [list_member_oid](list_member_oid.md)
  - lappend_oid
  - lfirst_oid
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [GetSchemaPublicationRelations](../G/GetSchemaPublicationRelations.md)
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md)
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)

## Notes and Other Information
- The function asserts that both input lists contain OID values using IsOidList()
- The original list1 is modified in place and returned, following PostgreSQL's typical list manipulation pattern
- Used extensively in publication-related functionality where OID lists need to be merged while maintaining uniqueness
- Part of PostgreSQL's generic list manipulation utilities in src/backend/nodes/list.c