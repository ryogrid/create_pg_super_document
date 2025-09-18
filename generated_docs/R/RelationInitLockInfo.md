# RelationInitLockInfo

## Location
src/backend/storage/lmgr/lmgr.c: 71 - 88

## Overview
RelationInitLockInfo initializes the lock information in a relation descriptor, setting up the lock identification data required for locking operations on the relation.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's relation caching system that initializes the locking metadata for a relation descriptor. It sets up the  structure within the relation's  field, which is essential for all subsequent locking operations on the relation.

The function determines the appropriate database ID for the lock based on whether the relation is shared across databases. For shared relations (system catalogs like pg_class, pg_type, etc.), it uses  as the database ID since these relations are accessible from all databases. For regular user relations, it uses  to ensure the lock is database-specific.

This initialization must be called during the creation of any relation descriptor to ensure that locking operations can function correctly.

## Parameters / Member Variables
- : A relation descriptor that needs its lock information initialized. Must be a valid relation with a valid OID.

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsValid
  - OidIsValid  
  - RelationGetRelid
  - MyDatabaseId
  - InvalidOid
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md) (src/backend/utils/cache/relcache.c:1252)
  - [formrdesc](../f/formrdesc.md) (src/backend/utils/cache/relcache.c:2004)
  - [RelationBuildLocalRelation](RelationBuildLocalRelation.md) (src/backend/utils/cache/relcache.c:3709)
  - [load_relcache_init_file](../l/load_relcache_init_file.md) (src/backend/utils/cache/relcache.c:6412)

## Notes and Other Information
- Must be called by relcache.c during creation of any relation descriptor
- The function includes assertions to ensure the relation and its OID are valid
- Critical for proper functioning of PostgreSQL's locking system
- Handles both shared and non-shared relations appropriately
- Part of the lock manager (lmgr) subsystem located in src/backend/storage/lmgr/lmgr.c:71-88