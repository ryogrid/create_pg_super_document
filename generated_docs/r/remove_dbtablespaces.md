# remove_dbtablespaces

## Location
[src/backend/commands/dbcommands.c:2964-3053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2964-L3053)

## Overview
Removes database directories from all tablespaces when a database is being dropped, ensuring complete cleanup of filesystem resources.

## Definition

```c
struct stat st;
```
## Detailed Description
This internal function systematically removes database-specific directories from all tablespaces in the PostgreSQL cluster. When a database is dropped, its data files exist in multiple tablespace directories, and this function ensures complete cleanup by iterating through every tablespace and removing the database's directory (identified by ) from each one.

The function performs a comprehensive tablespace scan, constructs the database path for each tablespace, verifies the directory exists, and removes it along with all contents. It also generates a WAL (Write-Ahead Log) record to ensure the filesystem changes are properly logged for crash recovery and replication purposes.

The function handles errors gracefully - if a directory doesn't exist or can't be removed completely, it continues processing other tablespaces and issues warnings rather than failing the entire operation.

## Parameters / Member Variables
- : The OID of the database whose tablespace directories should be removed

## Dependencies
- Functions called/Symbols referenced:
  -  - Open the pg_tablespace system catalog
  -  - Begin scanning the tablespace catalog
  -  - Get next tuple from table scan
  -  - Construct path to database directory in tablespace
  -  - Check if directory exists and get file status
  -  - Verify path is a directory
  -  - Recursively remove directory and contents
  -  - [Append](../A/Append.md) OID to list
  -  - Begin WAL record construction
  -  - Register data for WAL record
  -  - [Insert](../I/Insert.md) WAL record
  -  - End table scan
  -  - Close table
  -  - Free list memory
  -  - Free memory
  -  - Allocate memory
- Types referenced:
  -  - Structure for tablespace catalog entries
  -  - WAL record structure for database drop
- Called from:
  -  - Cleanup during failed database creation
  -  - Normal database drop operation

## Notes and Other Information
- This is a static (internal) function, not exposed in the public API
- Skips the global tablespace (GLOBALTABLESPACE_OID) as it's shared across all databases
- Issues warnings if some files cannot be removed but continues processing
- Generates WAL records with  type and  flag for proper crash recovery
- Uses  when scanning the tablespace catalog to avoid conflicts
- Memory management is handled carefully with  calls to prevent leaks
- The function is defined in 
- Critical for maintaining filesystem cleanliness and preventing orphaned database directories

## Simplified Source

```c
static void remove_dbtablespaces(Oid db_id) {
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    List *ltblspc = NIL;
    int ntblspc;
    Oid *tablespace_ids;

    // Scan all tablespaces
    rel = table_open(TableSpaceRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, NULL);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_tablespace spcform = (Form_pg_tablespace) GETSTRUCT(tuple);
        Oid dsttablespace = spcform->oid;
        char *dstpath;
        struct stat st;

        // Skip global tablespace
        if (dsttablespace == GLOBALTABLESPACE_OID)
            continue;

        // Get database path in this tablespace
        dstpath = GetDatabasePath(db_id, dsttablespace);

        // Check if directory exists
        if (lstat(dstpath, &st) < 0 || !S_ISDIR(st.st_mode)) {
            pfree(dstpath);
            continue;
        }

        // Remove directory tree
        if (!rmtree(dstpath, true))
            ereport(WARNING,
                    (errmsg("some useless files may be left behind in old database directory \"%s\"",
                            dstpath)));

        ltblspc = lappend_oid(ltblspc, dsttablespace);
        pfree(dstpath);
    }

    ntblspc = list_length(ltblspc);
    if (ntblspc == 0) {
        table_endscan(scan);
        table_close(rel, AccessShareLock);
        return;
    }

    // Convert list to array for WAL record
    tablespace_ids = (Oid *) palloc(ntblspc * sizeof(Oid));
    int i = 0;
    ListCell *cell;
    foreach(cell, ltblspc)
        tablespace_ids[i++] = lfirst_oid(cell);

    // Log filesystem change in WAL
    xl_dbase_drop_rec xlrec;
    xlrec.db_id = db_id;
    xlrec.ntablespaces = ntblspc;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, MinSizeOfDbaseDropRec);
    XLogRegisterData((char *) tablespace_ids, ntblspc * sizeof(Oid));
    XLogInsert(RM_DBASE_ID, XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE);

    // Cleanup
    list_free(ltblspc);
    pfree(tablespace_ids);
    table_endscan(scan);
    table_close(rel, AccessShareLock);
}
```