# binary_upgrade_replorigin_advance

## Location
[src/backend/utils/adt/pg_upgrade_support.c:369-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L369-L413)

## Overview
Updates the remote LSN position for a subscription's replication origin during binary upgrade operations.

## Definition
```c
Datum binary_upgrade_replorigin_advance(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure for logical replication. It advances the remote commit LSN for a subscription's replication origin, which is crucial for maintaining replication consistency across binary upgrades.

The function performs the following key operations:
1. Validates that the subscription name argument is not null
2. Extracts subscription name and optional remote commit LSN from function arguments
3. Opens the subscription catalog with exclusive lock
4. Resolves the subscription OID and constructs the corresponding replication origin name
5. Acquires a lock on the replication origin catalog to prevent concurrent modifications
6. Finds the replication origin by name
7. Advances the origin's remote commit position using replorigin_advance
8. Releases all acquired locks

The remote LSN advancement ensures that after the binary upgrade, logical replication will resume from the correct position, preventing data duplication or loss. The function includes a comment noting that origins will be flushed during shutdown checkpoint, ensuring persistence of the LSN values.

## Parameters / Member Variables
- `subname (text)`: Name of the subscription whose replication origin should be advanced
- `remote_commit (XLogRecPtr, optional)`: The remote commit LSN to advance to, defaults to InvalidXLogRecPtr if null

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE
  - PG_ARGISNULL
  - [text_to_cstring](../t/text_to_cstring.md)
  - PG_GETARG_TEXT_PP
  - PG_GETARG_LSN
  - [table_open](../t/table_open.md)
  - [get_subscription_oid](../g/get_subscription_oid.md)
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [replorigin_by_name](../r/replorigin_by_name.md)
  - [replorigin_advance](../r/replorigin_advance.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [table_close](../t/table_close.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct callers found (likely called via SQL during binary upgrades)

## Notes and Other Information
- This function is restricted to binary upgrade operations only via CHECK_IS_BINARY_UPGRADE
- The function includes null validation for the subscription name parameter
- Uses exclusive locks to prevent concurrent modifications during the operation
- The replorigin_advance call uses specific flags: backward=false, WAL log=false
- Origin values are flushed during shutdown checkpoint, ensuring persistence after upgrade
- Located in src/backend/utils/adt/pg_upgrade_support.c:369-413
- Critical for maintaining logical replication continuity across PostgreSQL binary upgrades

## Simplified Source

```c
Datum
binary_upgrade_replorigin_advance(PG_FUNCTION_ARGS)
{
    Relation rel;
    Oid subid;
    char *subname;
    char originname[NAMEDATALEN];
    RepOriginId node;
    XLogRecPtr remote_commit;

    // Ensure function runs only during binary upgrade
    CHECK_IS_BINARY_UPGRADE;

    // Validate subscription name is not null
    if (PG_ARGISNULL(0))
        elog(ERROR, "null argument to binary_upgrade_replorigin_advance is not allowed");

    // Extract function arguments
    subname = text_to_cstring(PG_GETARG_TEXT_PP(0));
    remote_commit = PG_ARGISNULL(1) ? InvalidXLogRecPtr : PG_GETARG_LSN(1);

    // Open subscription catalog and resolve subscription
    rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    subid = get_subscription_oid(subname, false);

    // Create replication origin name for this subscription
    ReplicationOriginNameForLogicalRep(subid, InvalidOid, originname, sizeof(originname));

    // Lock replication origin catalog and find the origin
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
    node = replorigin_by_name(originname, false);

    // Advance the replication origin to the specified LSN
    // This ensures replication resumes from correct position after upgrade
    replorigin_advance(node, remote_commit, InvalidXLogRecPtr,
                       false /* backward */, false /* WAL log */);

    // Release locks and clean up
    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);
    table_close(rel, RowExclusiveLock);

    PG_RETURN_VOID();
}
```