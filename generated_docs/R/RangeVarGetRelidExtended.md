# RangeVarGetRelidExtended

## Location
[src/backend/catalog/namespace.c:441-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L441-L653)

## Overview
A comprehensive function that resolves a RangeVar (relation name specification) to its actual OID, handling schema resolution, locking, and various error conditions with support for concurrent DDL operations.

## Definition

```c
Oid
RangeVarGetRelidExtended(const RangeVar *relation, LOCKMODE lockmode,
						 uint32 flags,
						 RangeVarGetRelidCallback callback, void *callback_arg)
```
## Detailed Description
RangeVarGetRelidExtended is the core function for resolving relation names to OIDs in PostgreSQL. It performs a sophisticated name lookup that handles schema qualification, temporary table resolution, and concurrent DDL safety through an invalidation message retry mechanism.

The function operates in a retry loop to handle concurrent DDL operations that might change the relation being looked up. It supports various behavioral flags for missing relations, lock waiting policies, and includes a callback mechanism for permission checks and additional processing.

Key features include:
- Cross-database reference validation (with appropriate error reporting)
- Special handling for temporary tables (RELPERSISTENCE_TEMP)  
- Schema-qualified and unqualified name resolution
- Invalidation message processing to handle concurrent DDL
- Flexible locking policies with NOWAIT and SKIP_LOCKED options
- Callback mechanism for custom validation logic

## Parameters / Member Variables
- `*relation`: RangeVar structure containing the relation name, optional schema name, and persistence information
- `lockmode`: Type of lock to acquire on the relation (or NoLock to skip locking)
- `flags`: Bitmask controlling behavior (RVR_MISSING_OK, RVR_NOWAIT, RVR_SKIP_LOCKED)
- `callback`: Optional function called after name resolution but before locking for custom validation
- `*callback_arg`: Argument passed to the callback function
## Dependencies
- Functions called/Symbols referenced:
  - [get_database_name](../g/get_database_name.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)  
  - [get_relname_relid](../g/get_relname_relid.md)
  - [RelnameGetRelid](RelnameGetRelid.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
- Called from (representative examples):
  - RangeVarGetRelid (inline wrapper)
  - [cluster](../c/cluster.md)
  - [LockTableCommand](../L/LockTableCommand.md)
  - [RemoveRelations](RemoveRelations.md)
  - [ExecuteTruncate](../E/ExecuteTruncate.md)

## Notes and Other Information
- Returns InvalidOid when relation is not found and RVR_MISSING_OK flag is set
- Implements sophisticated retry logic using SharedInvalidMessageCounter to handle concurrent DDL
- Special logic for temporary tables ensures they are found even when pg_temp is not first in search path
- Flags RVR_NOWAIT and RVR_SKIP_LOCKED are mutually exclusive
- The callback mechanism allows callers to perform permission checks before the relation is locked

## Simplified Source

```c
Oid RangeVarGetRelidExtended(const RangeVar *relation, LOCKMODE lockmode,
                            uint32 flags,
                            RangeVarGetRelidCallback callback, void *callback_arg)
{
    uint64 inval_count;
    Oid relId;
    Oid oldRelId = InvalidOid;
    bool retry = false;
    bool missing_ok = (flags & RVR_MISSING_OK) != 0;

    // Validate conflicting flags
    Assert(!((flags & RVR_NOWAIT) && (flags & RVR_SKIP_LOCKED)));

    // Reject cross-database references
    if (relation->catalogname) {
        if (strcmp(relation->catalogname, get_database_name(MyDatabaseId)) != 0)
            ereport(ERROR, /* cross-database error */);
    }

    // Retry loop to handle concurrent DDL operations
    for (;;) {
        // Track invalidation messages for DDL detection
        inval_count = SharedInvalidMessageCounter;

        // Resolve relation name to OID based on persistence and schema
        if (relation->relpersistence == RELPERSISTENCE_TEMP) {
            // Handle temporary table lookup
            if (relation->schemaname && namespaceId != myTempNamespace)
                ereport(ERROR, /* temp table schema error */);
            relId = get_relname_relid(relation->relname, myTempNamespace);
        }
        else if (relation->schemaname) {
            // Schema-qualified lookup
            Oid namespaceId = LookupExplicitNamespace(relation->schemaname, missing_ok);
            relId = get_relname_relid(relation->relname, namespaceId);
        }
        else {
            // Search namespace path
            relId = RelnameGetRelid(relation->relname);
        }

        // Invoke permission/validation callback
        if (callback)
            callback(relation, relId, oldRelId, callback_arg);

        // Skip locking if NoLock requested
        if (lockmode == NoLock)
            break;

        // Handle retry logic - same OID means we're done
        if (retry && relId == oldRelId)
            break;

        // Release old lock if OID changed
        if (retry && OidIsValid(oldRelId))
            UnlockRelationOid(oldRelId, lockmode);

        // Lock the relation (or accept invalidations if not found)
        if (!OidIsValid(relId)) {
            AcceptInvalidationMessages();
        }
        else if (!(flags & (RVR_NOWAIT | RVR_SKIP_LOCKED))) {
            LockRelationOid(relId, lockmode);
        }
        else if (!ConditionalLockRelationOid(relId, lockmode)) {
            // Handle lock failure based on flags
            int elevel = (flags & RVR_SKIP_LOCKED) ? DEBUG1 : ERROR;
            ereport(elevel, /* lock not available error */);
            return InvalidOid;
        }

        // If no invalidations occurred, we're done
        if (inval_count == SharedInvalidMessageCounter)
            break;

        // Prepare for retry
        retry = true;
        oldRelId = relId;
    }

    // Handle relation not found case
    if (!OidIsValid(relId) && !missing_ok) {
        ereport(ERROR, /* relation does not exist error */);
    }

    return relId;
}
```