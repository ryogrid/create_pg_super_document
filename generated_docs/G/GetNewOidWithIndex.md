# GetNewOidWithIndex

## Location
[src/backend/catalog/catalog.c:421-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L421-L529)

## Overview
GetNewOidWithIndex generates a new unique OID for insertion into a system catalog by checking against an existing unique index to avoid collisions.

## Definition

```c
Oid
GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
```
## Detailed Description
This function generates a unique OID for use in system catalogs by repeatedly calling GetNewObjectId() and checking for uniqueness against an existing index. It uses SnapshotAny to see uncommitted rows, reducing the risk of transient conflicts. The function includes comprehensive retry logic with exponential backoff logging to handle cases where many OIDs are already in use. It's designed for system catalogs which typically have relatively few entries compared to the full OID space. The function includes special handling for bootstrap mode and prevents OID generation for pg_type during pg_upgrade to avoid conflicts.

## Parameters / Member Variables
- : The system catalog relation where the new OID will be used
- : The OID of the unique index to check against for collisions
- : The attribute number of the OID column in the relation

## Dependencies
- Functions called/Symbols referenced:
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - IsBootstrapProcessingMode
  - [GetNewObjectId](GetNewObjectId.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - SnapshotAny
  - GETNEWOID_LOG_THRESHOLD
  - GETNEWOID_LOG_MAX_INTERVAL
- Called from (representative examples):
  - [toast_save_datum](../t/toast_save_datum.md) (src/backend/access/common/toast_internals.c:230, 284)
  - [GetNewRelFileNumber](GetNewRelFileNumber.md) (src/backend/catalog/catalog.c:577)
  - [TypeCreate](../T/TypeCreate.md) (src/backend/catalog/pg_type.c:481)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:576)
  - [OperatorCreate](../O/OperatorCreate.md) (src/backend/catalog/pg_operator.c:505)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md) (src/backend/catalog/pg_constraint.c:174)
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1405)
  - [CreateRole](../C/CreateRole.md) (src/backend/commands/user.c:475)
  - [CreateTableSpace](../C/CreateTableSpace.md) (src/backend/commands/tablespace.c:326)

## Notes and Other Information
- Only works with system relations (enforced by assertion)
- In bootstrap mode, falls back to simple GetNewObjectId() without uniqueness checking
- Uses SnapshotAny instead of SnapshotDirty to avoid issues with recently-deleted rows
- Implements sophisticated retry logging with exponential backoff to avoid log spam
- Assumes catalogs have relatively small numbers of entries (much less than 2^32)
- The race condition risk is minimal due to the large OID space and typical usage patterns
- Special protection against OID generation for pg_type during pg_upgrade operations

## Simplified Source

```c
Oid GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
    Oid newOid;
    SysScanDesc scan;
    ScanKeyData key;
    bool collides;
    uint64 retries = 0;
    uint64 retries_before_log = GETNEWOID_LOG_THRESHOLD;

    // Only system relations are supported
    Assert(IsSystemRelation(relation));

    // In bootstrap mode, we don't have any indexes to use
    if (IsBootstrapProcessingMode())
        return GetNewObjectId();

    // Never generate pg_type OID during pg_upgrade to avoid collisions
    Assert(!IsBinaryUpgrade || RelationGetRelid(relation) != TypeRelationId);

    // Generate new OIDs until we find one not in the table
    do {
        CHECK_FOR_INTERRUPTS();

        newOid = GetNewObjectId();

        // Set up scan key for the OID column
        ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
                   ObjectIdGetDatum(newOid));

        // Scan using SnapshotAny to see uncommitted rows
        scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
        collides = HeapTupleIsValid(systable_getnext(scan));
        systable_endscan(scan);

        // Log progress if we're retrying many times
        if (retries >= retries_before_log) {
            ereport(LOG, (errmsg("still searching for an unused OID in relation \"%s\"",
                                RelationGetRelationName(relation)),
                         errdetail_plural("OID candidates have been checked %llu time...",
                                        "OID candidates have been checked %llu times...",
                                        retries, (unsigned long long) retries)));

            // Exponential backoff for logging
            if (retries_before_log * 2 <= GETNEWOID_LOG_MAX_INTERVAL)
                retries_before_log *= 2;
            else
                retries_before_log += GETNEWOID_LOG_MAX_INTERVAL;
        }

        retries++;
    } while (collides);

    // Log completion if we had to retry
    if (retries > GETNEWOID_LOG_THRESHOLD) {
        ereport(LOG, (errmsg_plural("new OID has been assigned in relation \"%s\" after %llu retry",
                                   "new OID has been assigned in relation \"%s\" after %llu retries",
                                   retries, RelationGetRelationName(relation),
                                   (unsigned long long) retries)));
    }

    return newOid;
}
```