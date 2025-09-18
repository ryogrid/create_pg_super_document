# stack_address_present_add_flags

## Location
src/backend/catalog/dependency.c: 2692 - 2741

## Overview
Tests whether an object is present in an ObjectAddressStack (linked list) and if found, ORs additional flags into the object's associated data, implementing similar subobject relationship handling as its array counterpart.

## Definition


## Detailed Description
The  function provides stack-based equivalent functionality to , operating on a linked list structure (ObjectAddressStack) instead of an array. This function is used during dependency traversal to check for object presence while managing flags for complex object relationships.

Like its array counterpart, it handles three key scenarios:
1. **Exact match**: When both object and subobject IDs match, it ORs the provided flags into the stack entry's flags
2. **Subobject with whole object on stack**: When searching for a subobject but finding the whole object is already present, it returns true without flag propagation
3. **Whole object with subobject on stack**: When searching for a whole object but finding a subobject is present, it propagates flags to the subobject and marks it with DEPFLAG_SUBOBJECT

## Parameters / Member Variables
- : Pointer to the ObjectAddress to search for in the stack
- : Integer flags to OR into the found object's flag data
- : Pointer to the ObjectAddressStack (linked list) to search within and potentially modify

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressStack (struct type)
  - ObjectAddress (struct type)
  - DEPFLAG_SUBOBJECT (flag constant)
- Called from (representative examples):
  - find_expr_references_context (src/backend/catalog/dependency.c:174)
  - findDependentObjects (src/backend/catalog/dependency.c:469, 649)

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- Traverses the entire linked list structure to handle all potential matches
- Implements the same subobject relationship logic as  but for stack-based data structures
- Used during recursive dependency analysis where a stack-based approach is more appropriate than array-based storage
- Critical for preventing circular dependencies and managing proper deletion order in PostgreSQL's dependency system