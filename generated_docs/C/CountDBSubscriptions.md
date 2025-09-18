# CountDBSubscriptions

## Location
src/backend/catalog/pg_subscription.c: 123 - 154

## Overview
Counts the number of subscriptions defined in a specified database, primarily used by dropdb() to verify if a database can be safely dropped.

## Definition
```c
int CountDBSubscriptions(Oid dbid)
```

## Detailed Description
CountDBSubscriptions performs a system table scan on the pg_subscription catalog to count all subscription entries associated with a given database ID. It opens the subscription relation with a RowExclusiveLock, sets up a scan key to filter by database ID, and iterates through matching tuples to count them. This function is essential for database integrity checks, ensuring that databases with active subscriptions are not inadvertently dropped.

## Parameters / Member Variables
- `dbid`: The OID (Object Identifier) of the database for which to count subscriptions

## Dependencies
- Functions called/Symbols referenced:
  - table_open (open system table)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initialize scan key)
  - [systable_beginscan](../s/systable_beginscan.md) (begin system table scan)
  - [systable_getnext](../s/systable_getnext.md) (get next tuple from scan)
  - HeapTupleIsValid (validate heap tuple)
  - [systable_endscan](../s/systable_endscan.md) (end system table scan)
  - table_close (close system table)
- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (database drop command validation)

## Notes and Other Information
- Uses RowExclusiveLock when opening the subscription relation to prevent concurrent modifications during counting
- Performs a sequential scan through the pg_subscription catalog filtered by database ID
- Returns an integer count of matching subscriptions
- Critical for maintaining referential integrity when dropping databases
- Part of PostgreSQL's logical replication subscription management system
- The scan uses BTEqualStrategyNumber and F_OIDEQ for efficient OID-based lookups