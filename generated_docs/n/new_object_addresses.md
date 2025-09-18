# new_object_addresses

## Location
src/backend/catalog/dependency.c: 2487 - 2505

## Overview
Creates and initializes a new ObjectAddresses array structure used for managing expansible collections of ObjectAddress items in PostgreSQL's dependency tracking system.

## Definition


## Detailed Description
This function serves as a constructor for the ObjectAddresses data structure, which is used throughout PostgreSQL to maintain dynamic arrays of object references for dependency tracking and management. The function allocates memory for the main structure and initializes it with sensible defaults.

The ObjectAddresses structure is designed to grow dynamically as objects are added, starting with an initial capacity of 32 ObjectAddress entries. The 'extras' field is initialized to NULL and will only be allocated when additional metadata needs to be stored alongside the object addresses.

This is a fundamental utility function used extensively throughout the catalog system whenever collections of database objects need to be tracked, particularly during dependency analysis, object creation, and deletion operations.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - ObjectAddresses (struct type)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
- Called from (representative examples):
  - [performDeletion](../p/performDeletion.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - index_create
  - [AggregateCreate](../A/AggregateCreate.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [RemoveObjects](../R/RemoveObjects.md)

## Notes and Other Information
- Initializes with capacity of 32 ObjectAddress entries (maxrefs = 32)
- Sets numrefs to 0 (empty array initially)
- The 'extras' field remains NULL until additional metadata is needed
- Memory is allocated using palloc, so it will be automatically freed at end of transaction
- Part of the core dependency tracking infrastructure used throughout PostgreSQL
- Used extensively during DDL operations for tracking object relationships