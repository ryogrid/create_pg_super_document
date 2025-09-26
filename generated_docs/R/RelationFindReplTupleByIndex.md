# RelationFindReplTupleByIndex

## Location
[src/backend/executor/execReplication.c:176-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L176-L304)

## Overview
Searches for a specific tuple in a relation using an index scan, locks the tuple if found, and returns the tuple data in the output slot, designed specifically for replication scenarios.

## Definition
```c
bool RelationFindReplTupleByIndex(Relation rel, Oid idxoid, LockTupleMode lockmode, TupleTableSlot *searchslot, TupleTableSlot *outslot)
```

## Detailed Description
This function performs an index-based search for a tuple matching the values in the search slot, with built-in tuple locking and concurrency handling for replication scenarios. It is the primary interface for locating and locking tuples during logical replication operations.

The function works through several phases:
1. **Setup**: Opens the specified index with RowExclusiveLock and determines if the index is safe to skip duplicate checking (primary key or replica identity index)
2. **Scan Preparation**: Builds a scan key using `build_replindex_scan_key()` and starts an index scan with a dirty snapshot
3. **Tuple Search**: Iterates through matching index entries, performing equality checks when necessary for non-primary/non-replica-identity indexes
4. **Concurrency Handling**: Waits for any blocking transactions and retries the search if needed
5. **Tuple Locking**: Attempts to lock the found tuple in the requested mode, handling various concurrency scenarios including updates, deletes, and moved partitions

The function includes sophisticated retry logic to handle concurrent modifications, making it robust for multi-user environments where tuples may be modified during the search process.

## Parameters / Member Variables
- `rel`: The base relation to search in
- `idxoid`: OID of the index to use for the search
- `lockmode`: The tuple locking mode to apply if a matching tuple is found
- `searchslot`: TupleTableSlot containing the values to search for
- `outslot`: TupleTableSlot to store the found tuple data

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md) (to open the index relation)
  - [GetRelationIdentityOrPK](../G/GetRelationIdentityOrPK.md) (to check if index is primary key/replica identity)
  - InitDirtySnapshot (to initialize snapshot for scanning)
  - [build_replindex_scan_key](../b/build_replindex_scan_key.md) (to build the scan key)
  - [index_beginscan](../i/index_beginscan.md) (to start the index scan)
  - [index_rescan](../i/index_rescan.md) (to restart scan with keys)
  - [index_getnext_slot](../i/index_getnext_slot.md) (to retrieve tuples from index)
  - [tuples_equal](../t/tuples_equal.md) (to compare tuples when needed)
  - [ExecMaterializeSlot](../E/ExecMaterializeSlot.md) (to materialize the output slot)
  - [XactLockTableWait](../X/XactLockTableWait.md) (to wait for blocking transactions)
  - [table_tuple_lock](../t/table_tuple_lock.md) (to lock the found tuple)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md) (to get current command ID for locking)
  - [index_endscan](../i/index_endscan.md) (to end the index scan)
  - [index_close](../i/index_close.md) (to close the index relation)

- Called from (representative examples):
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - [exec_rt_fetch](../e/exec_rt_fetch.md)

## Notes and Other Information
- Returns true if a matching tuple was found and successfully locked, false otherwise
- Uses a dirty snapshot to see all committed and uncommitted changes
- Optimizes equality checking by skipping it for primary key and replica identity indexes
- Includes comprehensive retry logic for handling concurrent updates, deletes, and partition moves
- Maintains index lock until transaction commit for consistency
- Designed specifically for logical replication scenarios where precise tuple identification and locking is critical
- Handles moved partitions as a special case of concurrent updates