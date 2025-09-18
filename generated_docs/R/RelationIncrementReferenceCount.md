# RelationIncrementReferenceCount

## Location
src/backend/utils/cache/relcache.c: 2161 - 2173

## Overview
Increments the reference count for a given relation and tracks the reference in the current resource owner for proper cleanup.

## Definition


## Detailed Description
This function safely increments the reference count () of a relation structure by one and ensures that the current resource owner tracks this reference for proper cleanup. The function handles the special case of bootstrap mode, where reference count ownership tracking is disabled due to different initialization patterns during database bootstrap.

The function follows a two-step process:
1. Ensures the current resource owner has enough capacity to track an additional relation reference
2. Increments the relation's reference count and registers it with the resource owner (except in bootstrap mode)

## Parameters / Member Variables
- : Pointer to the Relation structure whose reference count should be incremented

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerEnlarge
  - IsBootstrapProcessingMode
  - ResourceOwnerRememberRelationRef
- Called from (representative examples):
  - heap_beginscan
  - index_beginscan_internal
  - PartitionDirectoryLookup
  - RelationIdGetRelation

## Notes and Other Information
- Bootstrap mode has special handling where reference count ownership tracking is disabled due to different relation lifecycle management during database initialization
- The function ensures resource owner capacity before making changes, preventing memory allocation failures during reference tracking
- This is part of PostgreSQL's reference counting mechanism to prevent premature cleanup of actively used relations
- Always paired with RelationDecrementReferenceCount when the relation reference is no longer needed