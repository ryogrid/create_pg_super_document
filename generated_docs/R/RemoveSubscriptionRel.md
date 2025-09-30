# RemoveSubscriptionRel

## Location
[src/backend/catalog/pg_subscription.c:416-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L416-L490)

## Overview
Removes subscription relation mapping entries from the pg_subscription_rel system catalog, supporting flexible deletion by subscription, relation, or both with validation for in-progress synchronization.

## Definition

```c
void
RemoveSubscriptionRel(Oid subid, Oid relid)
```
## Detailed Description
This function removes one or more entries from the pg_subscription_rel catalog based on the provided subscription and/or relation OIDs. It supports three deletion modes: removing all relations for a specific subscription (when relid is invalid), removing a specific relation from all subscriptions (when subid is invalid), or removing a specific subscription-relation pair (when both are valid).

The function includes important safety checks to prevent removal of relation mappings when table synchronization is in progress, unless the entire subscription is being updated. This prevents orphaned tablesync slots or origins from remaining in the system. It uses a catalog scan with appropriate scan keys to locate matching entries and deletes them using CatalogTupleDelete.

## Parameters / Member Variables
- : The OID of the subscription (can be InvalidOid to affect all subscriptions)
- : The OID of the relation (can be InvalidOid to affect all relations)

## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - ForwardScanDirection
  - Form_pg_subscription_rel
  - SUBREL_STATE_READY
  - [get_subscription_name](../g/get_subscription_name.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [table_endscan](../t/table_endscan.md)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [DropSubscription](../D/DropSubscription.md)

## Notes and Other Information
- Supports flexible deletion patterns: specific subscription-relation pairs, all relations for a subscription, or a relation from all subscriptions
- Implements safety validation to prevent removal during active table synchronization (unless removing entire subscription)
- Provides detailed error messages with hints for resolving synchronization conflicts
- Uses catalog scanning to efficiently locate and remove matching entries
- Critical for maintaining consistency when dropping tables or subscriptions in logical replication
- Located in src/backend/catalog/pg_subscription.c:416-490

## Simplified Source

```c
void
RemoveSubscriptionRel(Oid subid, Oid relid)
{
    Relation rel;
    TableScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple tup;
    int nkeys = 0;

    // Open subscription relation catalog table
    rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);

    // Set up scan keys based on provided parameters
    if (OidIsValid(subid)) {
        ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srsubid,
                   BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(subid));
    }

    if (OidIsValid(relid)) {
        ScanKeyInit(&skey[nkeys++], Anum_pg_subscription_rel_srrelid,
                   BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    }

    // Scan and delete matching entries
    scan = table_beginscan_catalog(rel, nkeys, skey);
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_subscription_rel subrel = (Form_pg_subscription_rel) GETSTRUCT(tup);

        // Safety check: prevent removal during active table synchronization
        // unless the entire subscription is being updated
        if (!OidIsValid(subid) && subrel->srsubstate != SUBREL_STATE_READY) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not drop relation mapping for subscription \"%s\"",
                                  get_subscription_name(subrel->srsubid, false)),
                           errdetail("Table synchronization for relation \"%s\" is in progress and is in state \"%c\".",
                                    get_rel_name(relid), subrel->srsubstate),
                           errhint("Use %s to enable subscription if not already enabled or use %s to drop the subscription.",
                                  "ALTER SUBSCRIPTION ... ENABLE",
                                  "DROP SUBSCRIPTION ...")));
        }

        // Delete the tuple
        CatalogTupleDelete(rel, &tup->t_self);
    }

    table_endscan(scan);
    table_close(rel, RowExclusiveLock);
}
```