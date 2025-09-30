# CreateDatabaseUsingFileCopy

## Location
[src/backend/commands/dbcommands.c:550-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L550-L669)

## Overview
CreateDatabaseUsingFileCopy implements the FILE_COPY strategy for creating a new database by copying each tablespace at the filesystem level and logging WAL records for each copied tablespace.

## Definition

```c
struct stat st;
```
## Detailed Description
This function creates a new database using the FILE_COPY strategy, which involves copying the entire source database's files at the filesystem level. The approach requires checkpoints before and after the copy operation, which may be expensive but greatly reduces WAL generation for large databases.

The function iterates through all tablespaces of the template database and copies each directory structure to the destination database. It handles tablespace remapping when the source and destination use different tablespaces. Each filesystem copy operation is logged as a single WAL record to ensure recoverability.

Two critical checkpoints are performed: one before copying to ensure all dirty buffers are flushed to disk and pending unlink requests are processed, and another after copying to avoid replay issues during crash recovery.

## Parameters / Member Variables
- : Object ID of the source (template) database to copy from
- : Object ID of the destination database being created  
- : Tablespace ID of the source database
- : Tablespace ID where the destination database should be created

## Dependencies
- Functions called/Symbols referenced:
  - [RequestCheckpoint](../R/RequestCheckpoint.md)
  - [table_open](../t/table_open.md), table_beginscan_catalog, table_endscan, table_close
  - [heap_getnext](../h/heap_getnext.md)
  - [GetDatabasePath](../G/GetDatabasePath.md)
  - [copydir](../c/copydir.md)
  - [directory_is_empty](../d/directory_is_empty.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert
  - [xl_dbase_create_file_copy_rec](../x/xl_dbase_create_file_copy_rec.md)
- Called from (representative examples):
  - [createdb](../c/createdb.md)

## Notes and Other Information
- This strategy is more efficient for large databases as it reduces WAL generation compared to WAL-logging strategies
- The function skips the global tablespace (GLOBALTABLESPACE_OID) and empty directories
- Critical checkpoints ensure consistency: the pre-copy checkpoint flushes all dirty buffers and processes pending unlinks, while the post-copy checkpoint ensures XLOG_DBASE_CREATE_FILE_COPY operations don't need replay during ordinary crash recovery
- The approach has documented limitations during PITR scenarios, particularly when base backups are being taken concurrently with database creation
- Alternative strategies like CreateDatabaseUsingWalLog() address some of the limitations of this file-copy approach

## Simplified Source

```c
static void CreateDatabaseUsingFileCopy(Oid src_dboid, Oid dst_dboid,
                                      Oid src_tsid, Oid dst_tsid) {
    TableScanDesc scan;
    Relation rel;
    HeapTuple tuple;

    // Force checkpoint to ensure source database is consistent on disk
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
                     CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL);

    // Scan all tablespaces and copy each one
    rel = table_open(TableSpaceRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, NULL);

    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_tablespace spaceform = (Form_pg_tablespace) GETSTRUCT(tuple);
        Oid srctablespace = spaceform->oid;
        Oid dsttablespace;
        char *srcpath, *dstpath;
        struct stat st;

        // Skip global tablespace
        if (srctablespace == GLOBALTABLESPACE_OID)
            continue;

        // Get source directory path
        srcpath = GetDatabasePath(src_dboid, srctablespace);

        // Skip if source doesn't exist or is empty
        if (stat(srcpath, &st) < 0 || !S_ISDIR(st.st_mode) ||
            directory_is_empty(srcpath)) {
            pfree(srcpath);
            continue;
        }

        // Determine destination tablespace (may be remapped)
        dsttablespace = (srctablespace == src_tsid) ? dst_tsid : srctablespace;
        dstpath = GetDatabasePath(dst_dboid, dsttablespace);

        // Copy directory structure
        copydir(srcpath, dstpath, false);

        // Log the filesystem copy operation in WAL
        xl_dbase_create_file_copy_rec xlrec;
        xlrec.db_id = dst_dboid;
        xlrec.tablespace_id = dsttablespace;
        xlrec.src_db_id = src_dboid;
        xlrec.src_tablespace_id = srctablespace;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xl_dbase_create_file_copy_rec));
        XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE);

        pfree(srcpath);
        pfree(dstpath);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    // Force final checkpoint to avoid replay issues during crash recovery
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
}
```