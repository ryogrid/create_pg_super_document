# xl_tblspc_drop_rec

## Location
[src/include/commands/tablespace.h:34-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/tablespace.h#L34-L37)

## Overview
A structure representing the WAL (Write-Ahead Log) record for tablespace drop operations, used to log the removal of a tablespace to ensure recoverability and consistency.

## Definition
```c
typedef struct xl_tblspc_drop_rec
{
    Oid         ts_id;
} xl_tblspc_drop_rec;
```

## Detailed Description
The `xl_tblspc_drop_rec` structure is a WAL record format designed for logging tablespace deletion operations. This simple structure is written to the transaction log when a tablespace is dropped, ensuring that the operation can be replayed during recovery scenarios such as crash recovery or streaming replication.

Unlike its counterpart `xl_tblspc_create_rec`, this structure is much simpler as it only needs to identify which tablespace to drop. The filesystem path is not needed since the drop operation only requires the tablespace OID to locate and remove the appropriate directory structure.

This record type is processed by the tablespace resource manager (RM_TBLSPC_ID) and plays a crucial role in maintaining consistency during tablespace management operations across PostgreSQL clusters.

## Parameters / Member Variables
- `ts_id`: The Object Identifier (Oid) of the tablespace being dropped, uniquely identifying the tablespace to be removed from the PostgreSQL cluster

## Dependencies
- Functions called/Symbols referenced:
  - (None directly referenced)

- Called from (representative examples):
  - DropTableSpace (src/backend/commands/tablespace.c:531, 536)
  - tblspc_redo (src/backend/commands/tablespace.c:1527)
  - tblspc_desc (src/backend/access/rmgrdesc/tblspcdesc.c:34)

## Notes and Other Information
- This structure is part of PostgreSQL's WAL logging system and is essential for crash recovery and replication
- The structure is intentionally minimal, containing only the necessary information to identify the tablespace to be dropped
- During WAL replay, this record is used to drop tablespaces on standby servers or during recovery operations
- The structure is associated with the XLOG_TBLSPC_DROP WAL record type
- The simplicity of this structure contrasts with the create record, reflecting the different information requirements of drop vs. create operations