# relmap_redo

## Location
[src/backend/utils/cache/relmapper.c:1096-1141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L1096-L1141)

## Overview
Handles replay of write-ahead log (WAL) records for relation mapping changes during PostgreSQL recovery, reconstructing relation-to-filenode mappings from logged updates.

## Definition

```c
struct the pathname for this database */
		dbpath = GetDatabasePath(xlrec->dbid, xlrec->tsid);
```
## Detailed Description
The  function is the resource manager routine responsible for replaying WAL records that contain relation mapping updates during database recovery. PostgreSQL maintains mapping files that associate logical relation OIDs with physical filenode numbers, and when these mappings change, they are logged to WAL for crash recovery purposes.

This function processes  records during recovery, extracting the new mapping data and writing it to the appropriate relmap file on disk. The function handles both shared catalog mappings (system-wide) and database-specific mappings, ensuring that the relation-to-filenode associations are correctly restored during recovery operations.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with relation mapping update information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecHasAnyBlockRefs  
  - XLogRecGetData
  - [GetDatabasePath](../G/GetDatabasePath.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [write_relmap_file](../w/write_relmap_file.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [pfree](../p/pfree.md)
  - elog
- Called from (representative examples):
  - MinSizeOfRelmapUpdate (referenced in header)

## Notes and Other Information
- Only processes  record types; panics on unknown operation codes
- Uses exclusive RelationMappingLock to prevent conflicts with concurrent relmap file loads during recovery
- Validates that WAL record data size matches expected RelMapFile structure size
- Does not write new WAL entries during replay (prevents infinite recursion)
- Handles both new database creation and existing database relmap updates with the same record type
- The function acquires locks and sends sinval messages even for new database creation cases, though unnecessary, for code simplicity
- Located in src/backend/utils/cache/relmapper.c:1096-1141

## Simplified Source

```c
void relmap_redo(XLogReaderState *record) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Only backup blocks are not used in relmap records
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_RELMAP_UPDATE) {
        xl_relmap_update *xlrec = (xl_relmap_update *) XLogRecGetData(record);
        RelMapFile newmap;
        char *dbpath;

        // Validate record size and copy mapping data
        if (xlrec->nbytes != sizeof(RelMapFile))
            elog(PANIC, "relmap_redo: wrong size %u in relmap update record", xlrec->nbytes);
        memcpy(&newmap, xlrec->data, sizeof(newmap));

        // Construct database path and write updated mapping file
        dbpath = GetDatabasePath(xlrec->dbid, xlrec->tsid);

        // Write relmap file with exclusive lock (no new WAL during replay)
        LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
        write_relmap_file(&newmap, false, true, false, xlrec->dbid, xlrec->tsid, dbpath);
        LWLockRelease(RelationMappingLock);

        pfree(dbpath);
    } else {
        elog(PANIC, "relmap_redo: unknown op code %u", info);
    }
}
```