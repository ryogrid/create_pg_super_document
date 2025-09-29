# RangeVarGetAndCheckCreationNamespace

## Location
[src/backend/catalog/namespace.c:739-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L739-L845)

## Overview
A comprehensive function that determines the target namespace for creating a new relation, performs permission checks, handles existing relation conflicts, and manages concurrent DDL safety through invalidation message processing.

## Definition
```c
Oid RangeVarGetAndCheckCreationNamespace(RangeVar *relation,
                                        LOCKMODE lockmode,
                                        Oid *existing_relation_id)
```

## Detailed Description
RangeVarGetAndCheckCreationNamespace extends the functionality of RangeVarGetCreationNamespace by adding comprehensive permission checking, existing relation detection, and proper locking mechanisms. It implements the same invalidation message retry pattern as RangeVarGetRelidExtended to handle concurrent DDL operations safely.

The function performs several critical operations:
- Validates cross-database references (not supported)
- Determines the creation namespace using RangeVarGetCreationNamespace
- Checks CREATE permissions on the target namespace
- Detects and optionally locks any existing relation with the same name
- Handles concurrent DDL through invalidation message tracking
- Adjusts relation persistence based on the target namespace

Key safety features include namespace locking to prevent schema dropping during the transaction and ownership verification before locking existing relations.

## Parameters / Member Variables
- `relation`: RangeVar structure describing the relation to be created (may be modified to set persistence)
- `lockmode`: Type of lock to acquire on existing relation (NoLock to skip locking)
- `existing_relation_id`: Output parameter set to OID of existing relation with same name, or InvalidOid if none exists

## Dependencies
- Functions called/Symbols referenced:
  - [get_database_name](../g/get_database_name.md)
  - [RangeVarGetCreationNamespace](RangeVarGetCreationNamespace.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - IsBootstrapProcessingMode
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [LockDatabaseObject](../L/LockDatabaseObject.md)
  - [UnlockDatabaseObject](../U/UnlockDatabaseObject.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [RangeVarAdjustRelationPersistence](RangeVarAdjustRelationPersistence.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [DefineSequence](../D/DefineSequence.md)
  - [DefineCompositeType](../D/DefineCompositeType.md)
  - [DefineVirtualRelation](../D/DefineVirtualRelation.md)

## Notes and Other Information
- Acquires AccessShareLock on target namespace to prevent concurrent schema drops
- Skips all permission checks and locking during bootstrap processing mode
- Only locks existing relations if the current user owns them (raises error otherwise)
- Modifies the input RangeVar to set appropriate persistence based on target namespace
- Uses retry logic with SharedInvalidMessageCounter to handle concurrent DDL operations
- Returns namespace OID and optionally sets existing relation OID through output parameter

## Simplified Source
```c
Oid RangeVarGetAndCheckCreationNamespace(RangeVar *relation,
                                        LOCKMODE lockmode,
                                        Oid *existing_relation_id) {
    Oid relid = InvalidOid;
    Oid nspid;
    uint64 inval_count;
    bool retry = false;

    // Reject cross-database references
    if (relation->catalogname) {
        if (strcmp(relation->catalogname, get_database_name(MyDatabaseId)) != 0) {
            ereport(ERROR, "cross-database references are not implemented");
        }
    }

    // Retry loop to handle concurrent DDL operations
    for (;;) {
        inval_count = SharedInvalidMessageCounter;

        // Get target namespace and check for existing relation
        nspid = RangeVarGetCreationNamespace(relation);
        if (existing_relation_id != NULL) {
            relid = get_relname_relid(relation->relname, nspid);
        }

        // Skip permission checks in bootstrap mode
        if (IsBootstrapProcessingMode()) {
            break;
        }

        // Check CREATE permission on namespace
        AclResult aclresult = object_aclcheck(NamespaceRelationId, nspid,
                                            GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK) {
            aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(nspid));
        }

        // Handle retry logic - release old locks if targets changed
        if (retry) {
            // Release locks if namespace or relation changed
            handle_lock_cleanup_on_retry();
        }

        // Acquire locks on namespace and existing relation
        LockDatabaseObject(NamespaceRelationId, nspid, 0, AccessShareLock);

        if (lockmode != NoLock && OidIsValid(relid)) {
            // Check ownership before locking existing relation
            if (!object_ownercheck(RelationRelationId, relid, GetUserId())) {
                aclcheck_error(ACLCHECK_NOT_OWNER,
                             get_relkind_objtype(get_rel_relkind(relid)),
                             relation->relname);
            }
            LockRelationOid(relid, lockmode);
        }

        // Exit if no invalidation messages processed
        if (inval_count == SharedInvalidMessageCounter) {
            break;
        }

        retry = true;
    }

    // Adjust relation persistence based on namespace type
    RangeVarAdjustRelationPersistence(relation, nspid);

    // Set output parameter
    if (existing_relation_id != NULL) {
        *existing_relation_id = relid;
    }

    return nspid;
}
```