# LogStandbyInvalidations

## Location
[src/backend/storage/ipc/standby.c:1462-1483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1462-L1483)

## Overview
Emits WAL records for cache invalidation messages, primarily used for commits without transaction IDs that contain invalidations.

## Definition

```c
void
LogStandbyInvalidations(int nmsgs, SharedInvalidationMessage *msgs,
						bool relcacheInitFileInval)
```
## Detailed Description
LogStandbyInvalidations creates WAL records containing cache invalidation messages that need to be replayed on standby servers. This function is specifically designed for commits that don't have assigned transaction IDs but still need to propagate cache invalidations to maintain consistency across standby servers.

The function constructs an xl_invalidations record containing the database ID, tablespace ID, relation cache initialization file invalidation flag, and the array of invalidation messages. This information is essential for standby servers to maintain proper cache consistency by invalidating the same cache entries that were invalidated on the primary server.

The WAL record uses the XLOG_INVALIDATIONS record type and includes both the header information and the actual invalidation message array.

## Parameters / Member Variables
- `nmsgs`: The number of invalidation messages in the msgs array
- `*msgs`: An array of SharedInvalidationMessage structures containing the invalidation information to be logged
- `relcacheInitFileInval`: Boolean flag indicating whether the relation cache initialization file should be invalidated
## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - XLOG_INVALIDATIONS
  - [xl_invalidations](../x/xl_invalidations.md)
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md)
  - MinSizeOfInvalidations
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)

## Notes and Other Information
- This function is specifically used for commits without transaction IDs that still contain cache invalidations
- The function captures the current database ID (MyDatabaseId) and tablespace ID (MyDatabaseTableSpace) to provide context for the invalidations
- Essential for maintaining cache consistency between primary and standby servers in Hot Standby configurations
- The xl_invalidations structure is zeroed before use to ensure clean initialization
- Both the header structure and the message array are registered as separate data chunks in the WAL record
- Located in src/backend/storage/ipc/standby.c:1462-1483

## Simplified Source

```c
void LogStandbyInvalidations(int nmsgs, SharedInvalidationMessage *msgs,
                            bool relcacheInitFileInval) {
    xl_invalidations xlrec;

    // Prepare the invalidation record header
    memset(&xlrec, 0, sizeof(xlrec));
    xlrec.dbId = MyDatabaseId;
    xlrec.tsId = MyDatabaseTableSpace;
    xlrec.relcacheInitFileInval = relcacheInitFileInval;
    xlrec.nmsgs = nmsgs;

    // Write to WAL: header followed by invalidation messages
    XLogBeginInsert();
    XLogRegisterData((char *) (&xlrec), MinSizeOfInvalidations);
    XLogRegisterData((char *) msgs, nmsgs * sizeof(SharedInvalidationMessage));
    XLogInsert(RM_STANDBY_ID, XLOG_INVALIDATIONS);
}
```