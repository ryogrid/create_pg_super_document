# pg_replication_origin_advance

## Location
[src/backend/replication/logical/origin.c:1456-1490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1456-L1490)

## Overview
Advances the replication progress for a specified replication origin by name to a given remote LSN position, typically used for setting up initial replication state.

## Definition
```c
Datum pg_replication_origin_advance(PG_FUNCTION_ARGS)
```

## Detailed Description
This function updates the replication progress for a specific replication origin identified by name. It advances the recorded remote LSN to the specified position, effectively marking that all transactions up to that point have been successfully replicated. 

The function performs several important operations:
1. Looks up the replication origin by name to get its internal ID
2. Acquires a row-exclusive lock on the replication origin catalog to prevent concurrent modifications
3. Calls the internal replorigin_advance() function with specific parameters for setup scenarios
4. Uses InvalidXLogRecPtr for the local commit LSN since this is intended for initial setup rather than transaction replay

The function is designed primarily for setting up initial replication state and should not be used during normal transaction replay, as noted in the comments. It allows backward movement of the progress position and logs the advancement to WAL.

## Parameters / Member Variables
- `name` (text): The name of the replication origin to advance
- `remote_commit` (XLogRecPtr): The remote LSN position to advance to

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_LSN
  - [replorigin_check_prerequisites](../r/replorigin_check_prerequisites.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [replorigin_by_name](../r/replorigin_by_name.md)
  - [text_to_cstring](../t/text_to_cstring.md)
  - [replorigin_advance](../r/replorigin_advance.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Acquires RowExclusiveLock on ReplicationOriginRelationId to prevent origin from being dropped during the operation
- Uses InvalidXLogRecPtr for local commit LSN, making it unsuitable for transaction replay scenarios
- Allows backward movement of replication progress (go_backward = true)
- Logs the advancement to WAL (wal_log = true)
- Intended for initial replication state setup, not for ongoing transaction replay
- The function comments explicitly warn against using this for replay scenarios due to the InvalidXLogRecPtr local commit parameter
- Located in src/backend/replication/logical/origin.c:1456-1490

## Simplified Source

```c
Datum pg_replication_origin_advance(PG_FUNCTION_ARGS) {
    text *name = PG_GETARG_TEXT_PP(0);
    XLogRecPtr remote_commit = PG_GETARG_LSN(1);
    RepOriginId node;

    // Check prerequisites: require slots and disallow during recovery
    replorigin_check_prerequisites(true, false);

    // Lock replication origin catalog to prevent concurrent changes
    LockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    // Look up origin by name
    node = replorigin_by_name(text_to_cstring(name), false);

    // Advance replication progress for setup scenarios
    // Note: Uses InvalidXLogRecPtr for local commit - not suitable for replay
    replorigin_advance(node, remote_commit, InvalidXLogRecPtr,
                      true /* go backward */, true /* WAL log */);

    UnlockRelationOid(ReplicationOriginRelationId, RowExclusiveLock);

    PG_RETURN_VOID();
}
```