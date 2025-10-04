# get_subscription_list

## Location
[src/backend/replication/logical/launcher.c:112-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L112-L182)

## Overview
Retrieves a list of all active logical replication subscriptions from the pg_subscription catalog, filtering for fields relevant to worker start/stop operations.

## Definition

```c
static List *
get_subscription_list(void)
```
## Detailed Description
The get_subscription_list function scans the pg_subscription system catalog to build a list of all subscriptions in the database. It extracts essential subscription information needed by the logical replication launcher to manage worker processes. The function operates within its own transaction context to ensure consistent reads from the catalog while carefully managing memory allocation to prevent leaks.

The function uses a heap scan over the pg_subscription table and creates Subscription structures containing only the fields necessary for worker management: oid, database id, owner, enabled status, and name. Memory allocation is performed in the caller's context rather than the transaction context to ensure the results persist beyond the transaction's lifetime.

## Parameters / Member Variables
- Returns:  - A list of Subscription structures containing essential subscription information

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)  
  - [table_open](../t/table_open.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_close](../t/table_close.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - [pstrdup](../p/pstrdup.md)
  - [lappend](../l/lappend.md)
- Called from:
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md)

## Notes and Other Information
- The function includes a FIXME comment noting that the snapshot handling may not reliably prevent HOT pruning as intended
- Memory context switching is used within the scan loop to allocate results in the caller's context while preventing leaks from heap operations
- Only fills subscription fields relevant to worker start/stop operations, leaving other fields uninitialized for efficiency
- Operates under AccessShareLock on the pg_subscription relation to allow concurrent reads

## Simplified Source

```c
static List *get_subscription_list(void)
{
    List *res = NIL;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    MemoryContext resultcxt = CurrentMemoryContext;

    // Start transaction and get snapshot for consistent catalog reads
    StartTransactionCommand();
    (void) GetTransactionSnapshot();

    // Scan pg_subscription catalog
    rel = table_open(SubscriptionRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);
        Subscription *sub;
        MemoryContext oldcxt;

        // Allocate result in caller's context to persist beyond transaction
        oldcxt = MemoryContextSwitchTo(resultcxt);

        // Extract essential subscription info for worker management
        sub = (Subscription *) palloc0(sizeof(Subscription));
        sub->oid = subform->oid;
        sub->dbid = subform->subdbid;
        sub->owner = subform->subowner;
        sub->enabled = subform->subenabled;
        sub->name = pstrdup(NameStr(subform->subname));

        res = lappend(res, sub);
        MemoryContextSwitchTo(oldcxt);
    }

    // Clean up scan and transaction
    table_endscan(scan);
    table_close(rel, AccessShareLock);
    CommitTransactionCommand();

    return res;
}
```