# SetLocktagRelationOid

## Location
src/backend/storage/lmgr/lmgr.c: 89 - 107

## Overview
SetLocktagRelationOid sets up a locktag structure for a relation using only the relation OID, determining the appropriate database ID based on whether the relation is shared.

## Definition
```c
static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid)
```

## Detailed Description
This static inline function is a utility that constructs a LOCKTAG structure for relation-level locking operations. It takes a relation OID and populates the provided LOCKTAG structure with the appropriate database ID and relation ID.

The function automatically determines the correct database ID by checking if the relation is shared across databases using IsSharedRelation(). For shared system relations (like pg_class, pg_type), it sets the database ID to InvalidOid since these relations are accessible from all databases. For regular user relations, it uses MyDatabaseId to make the lock database-specific.

This function is primarily used internally by other locking functions to standardize the creation of relation locktags before acquiring or releasing locks.

## Parameters / Member Variables
- `tag`: Pointer to a LOCKTAG structure that will be populated with the relation lock information
- `relid`: The OID of the relation for which to create the locktag

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - SET_LOCKTAG_RELATION
  - InvalidOid
  - MyDatabaseId
- Called from (representative examples):
  - [LockRelationOid](../L/LockRelationOid.md) (src/backend/storage/lmgr/lmgr.c:114)
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md) (src/backend/storage/lmgr/lmgr.c:157)
  - [UnlockRelationOid](../U/UnlockRelationOid.md) (src/backend/storage/lmgr/lmgr.c:231)
  - [CheckRelationOidLockedByMe](../C/CheckRelationOidLockedByMe.md) (src/backend/storage/lmgr/lmgr.c:351)

## Notes and Other Information
- Static inline function for internal use within the lock manager
- Handles both shared and non-shared relations automatically
- Essential utility for relation locking operations
- Uses the SET_LOCKTAG_RELATION macro to populate the locktag structure
- Part of the lock manager (lmgr) subsystem located in src/backend/storage/lmgr/lmgr.c:89-107