# free_object_addresses

## Location
[src/backend/catalog/dependency.c:2773-2784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2773-L2784)

## Overview
A cleanup function that properly deallocates memory used by an ObjectAddresses structure, including both the array of object references and optional extras array.

## Definition

```c
void
free_object_addresses(ObjectAddresses *addrs)
```
## Detailed Description
This function performs proper memory deallocation for an ObjectAddresses structure when it is no longer needed. The ObjectAddresses structure contains dynamically allocated arrays that hold object references and optional extra information used in PostgreSQL's dependency tracking system. The function ensures that all allocated memory is freed in the correct order - first the contained arrays, then the structure itself.

## Parameters / Member Variables
- : Pointer to the ObjectAddresses structure to be freed. The structure contains:
  - : Array of ObjectAddress entries that gets freed
  - : Optional array of ObjectAddressExtra entries that gets freed if present
  -  and : Size tracking fields (no cleanup needed)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - ObjectAddresses (structure type)
- Called from (representative examples):
  - [performDeletion](../p/performDeletion.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)  
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [index_create](../i/index_create.md)
  - [AggregateCreate](../A/AggregateCreate.md)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [RemoveObjects](../R/RemoveObjects.md)

## Notes and Other Information
- This is a critical cleanup function used throughout PostgreSQL's dependency management system
- The function safely handles the case where  might be NULL by checking before freeing
- Always called after dependency operations are complete to prevent memory leaks
- Part of PostgreSQL's dependency tracking infrastructure located in src/backend/catalog/dependency.c