# xl_dbase_create_file_copy_rec

## Location
[src/include/commands/dbcommands_xlog.h:29-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/dbcommands_xlog.h#L29-L35)

## Overview
WAL record structure used to log an entire CREATE DATABASE operation when using the FILE_COPY strategy, containing identifiers for source and target databases and tablespaces.

## Definition

```c
typedef struct xl_dbase_create_file_copy_rec
{
	Oid			db_id;
	Oid			tablespace_id;
	Oid			src_db_id;
	Oid			src_tablespace_id;
} xl_dbase_create_file_copy_rec;
```
## Detailed Description
The xl_dbase_create_file_copy_rec structure represents a Write-Ahead Log (WAL) record for database creation operations that use the FILE_COPY strategy. This strategy involves physically copying files from a template database rather than logging individual block changes. The entire CREATE DATABASE operation is captured in a single WAL record, making it efficient for large template databases.

This record type is identified by the XLOG_DBASE_CREATE_FILE_COPY (0x00) record type and contains all the necessary information to recreate or undo the database creation operation during WAL replay.

## Parameters / Member Variables
- : OID of the newly created database
- : OID of the tablespace where the new database is created
- : OID of the source/template database being copied from
- : OID of the tablespace containing the source database

## Dependencies
- Functions called/Symbols referenced: None (struct definition only)
- Called from (representative examples):
  - [CreateDatabaseUsingFileCopy](../C/CreateDatabaseUsingFileCopy.md) (dbcommands.c:615, 624)
  - [movedb](../m/movedb.md) (dbcommands.c:2165, 2174)
  - [dbase_redo](../d/dbase_redo.md) (dbcommands.c:3279, 3280)
  - [dbase_desc](../d/dbase_desc.md) (dbasedesc.c:29, 30)
  - [XLogPrefetcherNextBlock](../X/XLogPrefetcherNextBlock.md) (xlogprefetcher.c:567, 568)
  - [SummarizeDbaseRecord](../S/SummarizeDbaseRecord.md) (walsummarizer.c:1273, 1277)

## Notes and Other Information
- Part of the database resource manager XLOG system for create/drop database operations
- Used specifically with the FILE_COPY strategy, as opposed to WAL_LOG strategy which uses xl_dbase_create_wal_log_rec
- The FILE_COPY strategy is more efficient when creating databases from large templates since it avoids logging every individual block
- During WAL replay, this record enables proper reconstruction of the database creation operation including tablespace mappings