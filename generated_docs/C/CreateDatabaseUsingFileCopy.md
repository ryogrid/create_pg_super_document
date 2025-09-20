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
  - table_open, table_beginscan_catalog, table_endscan, table_close
  - [heap_getnext](../h/heap_getnext.md)
  - [GetDatabasePath](../G/GetDatabasePath.md)
  - copydir
  - [directory_is_empty](../d/directory_is_empty.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert
  - xl_dbase_create_file_copy_rec
- Called from (representative examples):
  - [createdb](../c/createdb.md)

## Notes and Other Information
- This strategy is more efficient for large databases as it reduces WAL generation compared to WAL-logging strategies
- The function skips the global tablespace (GLOBALTABLESPACE_OID) and empty directories
- Critical checkpoints ensure consistency: the pre-copy checkpoint flushes all dirty buffers and processes pending unlinks, while the post-copy checkpoint ensures XLOG_DBASE_CREATE_FILE_COPY operations don't need replay during ordinary crash recovery
- The approach has documented limitations during PITR scenarios, particularly when base backups are being taken concurrently with database creation
- Alternative strategies like CreateDatabaseUsingWalLog() address some of the limitations of this file-copy approach