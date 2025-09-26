# xl_dbase_create_wal_log_rec

## Location
[src/include/commands/dbcommands_xlog.h:42-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/dbcommands_xlog.h#L42-L46)

## Overview
WAL record structure used to log the beginning of a CREATE DATABASE operation when using the WAL_LOG strategy, where individual blocks are logged separately afterward.

## Definition

```c
typedef struct xl_dbase_create_wal_log_rec
{
	Oid			db_id;
	Oid			tablespace_id;
} xl_dbase_create_wal_log_rec;
```
## Detailed Description
The xl_dbase_create_wal_log_rec structure represents a Write-Ahead Log (WAL) record that marks the beginning of a database creation operation using the WAL_LOG strategy. Unlike the FILE_COPY strategy which logs the entire operation in a single record, the WAL_LOG strategy logs the initial database creation parameters in this record, then logs each individual data block separately as subsequent WAL records.

This approach is used when creating databases from templates where individual block-level logging is preferred, typically for smaller template databases or when more granular WAL replay control is desired. The record type is identified by XLOG_DBASE_CREATE_WAL_LOG (0x10).

## Parameters / Member Variables
- `db_id`: OID of the newly created database
- `tablespace_id`: OID of the tablespace where the new database is being created
## Dependencies
- Functions called/Symbols referenced: None (struct definition only)
- Called from (representative examples):
  - [CreateDirAndVersionFile](../C/CreateDirAndVersionFile.md) (dbcommands.c:524, 533)
  - [dbase_redo](../d/dbase_redo.md) (dbcommands.c:3351, 3352)
  - [dbase_desc](../d/dbase_desc.md) (dbasedesc.c:38, 39)
  - [SummarizeDbaseRecord](../S/SummarizeDbaseRecord.md) (walsummarizer.c:1285, 1288)

## Notes and Other Information
- Part of the database resource manager XLOG system for create/drop database operations
- Used specifically with the WAL_LOG strategy, complementing xl_dbase_create_file_copy_rec which is used with the FILE_COPY strategy
- This record only contains the target database information (db_id, tablespace_id) unlike the FILE_COPY variant which also includes source database information
- After this record is logged, individual data blocks from the template database are logged separately, allowing for more granular WAL replay and recovery
- The WAL_LOG strategy provides better control and visibility into the database creation process at the cost of potentially more WAL volume