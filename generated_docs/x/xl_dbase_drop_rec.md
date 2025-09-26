# xl_dbase_drop_rec

## Location
src/include/commands/dbcommands_xlog.h: 48 - 53

## Overview
WAL record structure used to log DROP DATABASE operations, containing the database OID and variable-length array of tablespace OIDs where database files need to be removed.

## Definition


## Detailed Description
The xl_dbase_drop_rec structure represents a Write-Ahead Log (WAL) record for database drop operations. This record contains all the information necessary to properly remove a database during normal operation or to replay the drop operation during WAL recovery. 

The structure uses a flexible array member to accommodate databases that span multiple tablespaces, making the record size variable depending on how many tablespaces contain files for the database being dropped. This design efficiently handles both simple single-tablespace databases and complex multi-tablespace configurations.

The record type is identified by XLOG_DBASE_DROP (0x20) and ensures that all database files across all relevant tablespaces are properly removed during the drop operation.

## Parameters / Member Variables
- : OID of the database being dropped
- : Number of tablespace IDs in the tablespace_ids array, indicating how many tablespaces contain files for this database
- : Variable-length array containing the OIDs of all tablespaces that contain files for the database being dropped

## Dependencies
- Functions called/Symbols referenced: 
  - FLEXIBLE_ARRAY_MEMBER (macro)
  - offsetof (used in MinSizeOfDbaseDropRec)
- Called from (representative examples):
  - movedb (dbcommands.c:2263, 2269)
  - remove_dbtablespaces (dbcommands.c:3021)
  - dbase_redo (dbcommands.c:3370)
  - dbase_desc (dbasedesc.c:46)
  - SummarizeDbaseRecord (walsummarizer.c:1296, 1300)

## Notes and Other Information
- Part of the database resource manager XLOG system for create/drop database operations
- Uses FLEXIBLE_ARRAY_MEMBER to support variable-length tablespace ID arrays
- MinSizeOfDbaseDropRec macro provides the minimum size of the record (without any tablespace IDs), useful for memory allocation and parsing
- The variable-length design accommodates databases that may have relations scattered across multiple tablespaces
- During WAL replay, this record ensures that database files are properly removed from all relevant tablespaces
- Essential for maintaining database consistency during crash recovery scenarios involving database drops