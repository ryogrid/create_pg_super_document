# xl_tblspc_create_rec

## Location
[src/include/commands/tablespace.h:28-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/tablespace.h#L28-L32)

## Overview
A structure representing the WAL (Write-Ahead Log) record for tablespace creation operations, used to log the creation of a tablespace to ensure recoverability and consistency.

## Definition
```c
typedef struct xl_tblspc_create_rec
{
    Oid         ts_id;
    char        ts_path[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated string */
} xl_tblspc_create_rec;
```

## Detailed Description
The `xl_tblspc_create_rec` structure is a WAL record format specifically designed for logging tablespace creation operations. This structure is written to the transaction log when a new tablespace is created, ensuring that the operation can be replayed during recovery scenarios such as crash recovery or streaming replication.

The structure uses a flexible array member for the path component, allowing it to accommodate tablespace paths of varying lengths while maintaining efficient storage. The actual size of the WAL record varies depending on the length of the tablespace path.

This record type is processed by the tablespace resource manager (RM_TBLSPC_ID) and is essential for maintaining consistency across PostgreSQL clusters in master-standby configurations.

## Parameters / Member Variables
- `ts_id`: The Object Identifier (Oid) of the tablespace being created, uniquely identifying the tablespace within the PostgreSQL cluster
- `ts_path`: A variable-length, null-terminated string containing the filesystem path where the tablespace directory will be located

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - [CreateTableSpace](../C/CreateTableSpace.md) (src/backend/commands/tablespace.c:361, 367)
  - [tblspc_redo](../t/tblspc_redo.md) (src/backend/commands/tablespace.c:1520)
  - [tblspc_desc](../t/tblspc_desc.md) (src/backend/access/rmgrdesc/tblspcdesc.c:28)

## Notes and Other Information
- This structure is part of PostgreSQL's WAL logging system and is critical for crash recovery and replication
- The flexible array member design allows efficient storage of variable-length tablespace paths
- During WAL replay, this record is used to recreate tablespaces on standby servers or during recovery
- The structure is tightly coupled with the XLOG_TBLSPC_CREATE WAL record type
- Care must be taken when handling the variable-length nature of this structure in memory operations