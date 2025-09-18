# object_address_present

## Location
src/backend/catalog/dependency.c: 2593 - 2618

## Overview
Tests whether a specific database object is already present in an ObjectAddresses array, with support for subobject detection.

## Definition


## Detailed Description
The  function performs a lookup to determine if a given ObjectAddress is already contained within an ObjectAddresses array. The function implements a reverse iteration strategy (from the end of the array backward) for efficiency reasons, as newer additions are typically found at the end.

A key feature of this function is its intelligent subobject handling: if the target object has a specific subobject ID but an entry in the array has  (representing the entire object), the function returns true. This means that if the whole object is already referenced, any specific subobject is considered present as well.

## Parameters / Member Variables
- : Pointer to the ObjectAddress to search for in the array
- : Pointer to the ObjectAddresses array to search within

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddresses (struct type)
  - ObjectAddress (struct type)
- Called from (representative examples):
  - findDependentObjects (src/backend/catalog/dependency.c:617)
  - AlterConstraintNamespaces (src/backend/catalog/pg_constraint.c:784)
  - AlterRelationNamespaceInternal (src/backend/commands/tablecmds.c:17342)
  - AlterIndexNamespaces (src/backend/commands/tablecmds.c:17417)
  - AlterTypeNamespaceInternal (src/backend/commands/typecmds.c:4177)

## Notes and Other Information
- The function uses reverse iteration (i = numrefs - 1 to i >= 0) which suggests that recently added objects are more likely to be found, optimizing for common access patterns
- The subobject logic (objectSubId handling) prevents duplicate entries when both a whole object and its subobjects are referenced
- Returns true if either an exact match is found or if the object is a subobject of an entry with objectSubId = 0
- This function is critical for dependency management and avoiding duplicate object references in PostgreSQL's catalog system