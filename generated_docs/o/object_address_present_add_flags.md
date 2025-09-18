# object_address_present_add_flags

## Location
src/backend/catalog/dependency.c: 2619 - 2691

## Overview
Tests whether an object is present in an ObjectAddresses array and if found, ORs additional flags into the object's associated extra data, handling complex subobject relationships during dependency analysis.

## Definition


## Detailed Description
The  function extends the basic presence check of  by also modifying flag data when objects are found. This function is critical for dependency management during object deletion operations, particularly when dealing with complex relationships between whole objects and their subobjects (like tables and columns).

The function handles three distinct scenarios:
1. **Exact match**: When both object and subobject IDs match exactly, it ORs the provided flags into the existing flags
2. **Subobject superseded by whole object**: When looking for a subobject but finding the whole object is already marked for deletion, it reports the subobject as found without modifying flags
3. **Whole object after subobject**: When looking for a whole object but finding a subobject is already marked, it marks the subobject with additional flags including DEPFLAG_SUBOBJECT to prevent separate reporting

## Parameters / Member Variables
- : Pointer to the ObjectAddress to search for
- : Integer flags to OR into the found object's extra data
- : Pointer to the ObjectAddresses array to search within and potentially modify

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddresses (struct type)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
  - ObjectAddressExtra (struct type)
  - DEPFLAG_SUBOBJECT (flag constant)
- Called from (representative examples):
  - find_expr_references_context (src/backend/catalog/dependency.c:171)
  - [findDependentObjects](../f/findDependentObjects.md) (src/backend/catalog/dependency.c:487, 720)

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- The function cannot exit early when handling whole object vs subobject conflicts, requiring a full array scan in some cases
- Includes sophisticated logic for handling table/column deletion ordering to prevent dropping datatypes before tables
- The flags parameter can be 0 for read-only probing without modification
- Critical for maintaining consistent dependency deletion order in PostgreSQL's CASCADE operations