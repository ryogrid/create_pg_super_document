# pgstat_create_transactional

## Location
[src/backend/utils/activity/pgstat_xact.c:357-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L357-L378)

## Overview
Creates a statistics entry for a newly created database object in a transactional manner, ensuring that if the current transaction or sub-transaction aborts, the stats entry will also be dropped.

## Definition
```c
void pgstat_create_transactional(PgStat_Kind kind, Oid dboid, Oid objoid)
```

## Detailed Description
This function creates a statistics entry for a database object (relation, function, subscription, etc.) with transactional semantics. The key behavior is that if the current transaction or sub-transaction is aborted, the statistics entry creation will be rolled back as well.

The function first checks if a statistics entry already exists for the specified object. If one exists, it issues a WARNING and resets the existing statistics before proceeding with the creation. This ensures a clean slate for the new statistics entry.

The actual transactional behavior is implemented by delegating to `create_drop_transactional_internal`, which registers the operation in a transaction-local pending operations list that will be processed during transaction commit or abort.

## Parameters / Member Variables
- `kind`: The type of PostgreSQL statistics object being created (e.g., relation, function, subscription)
- `dboid`: The OID of the database containing the object
- `objoid`: The OID of the specific object for which statistics are being created

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - [pgstat_reset](pgstat_reset.md)
  - [create_drop_transactional_internal](../c/create_drop_transactional_internal.md)
  - [PgStat_Kind](../P/PgStat_Kind.md) (type)
- Called from (representative examples):
  - [pgstat_create_function](pgstat_create_function.md)
  - [pgstat_create_relation](pgstat_create_relation.md)
  - [pgstat_create_subscription](pgstat_create_subscription.md)

## Notes and Other Information
- Located in src/backend/utils/activity/pgstat_xact.c:357-378
- Issues a WARNING if an existing statistics entry is found and resets it before creating the new one
- The transactional nature means the statistics creation is tied to the success of the current transaction
- Works in conjunction with `pgstat_drop_transactional` to provide complete transactional semantics for statistics management
- The function uses TopTransactionContext for memory allocation to ensure proper cleanup on transaction abort

## Simplified Source

```c
void pgstat_create_transactional(PgStat_Kind kind, Oid dboid, Oid objoid) {
    // Check if statistics entry already exists
    if (pgstat_get_entry_ref(kind, dboid, objoid, false, NULL)) {
        // Warn and reset existing statistics
        ereport(WARNING,
                errmsg("resetting existing statistics for kind %s, db=%u, oid=%u",
                       (pgstat_get_kind_info(kind))->name, dboid, objoid));

        pgstat_reset(kind, dboid, objoid);
    }

    // Create the statistics entry with transactional semantics
    create_drop_transactional_internal(kind, dboid, objoid, /* create */ true);
}
```