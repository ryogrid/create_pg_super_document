# log_smgrcreate

## Location
[src/backend/catalog/storage.c:186-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L186-L205)

## Overview
log_smgrcreate writes a WAL record documenting the creation of a storage manager file, ensuring crash recovery can recreate the physical file if needed.

## Definition
```c
void log_smgrcreate(const RelFileLocator *rlocator, ForkNumber forkNum)
```

## Detailed Description
log_smgrcreate performs XLogInsert of an XLOG_SMGR_CREATE record to the Write-Ahead Log, creating a permanent record that a relation file has been created. This function is called during relation creation operations to ensure that the physical file creation can be replayed during crash recovery. The WAL record contains the complete RelFileLocator information and fork number, allowing the recovery process to recreate the exact same file structure. The record uses the XLR_SPECIAL_REL_UPDATE flag to indicate this is a special operation affecting relation storage.

## Parameters / Member Variables
- `rlocator`: Pointer to RelFileLocator structure containing tablespace OID, database OID, relation OID, and other location information for the file being created
- `forkNum`: ForkNumber identifying which fork of the relation is being created (main, FSM, visibility map, or init fork)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - xl_smgr_create
  - XLOG_SMGR_CREATE
  - XLR_SPECIAL_REL_UPDATE
- Called from (representative examples):
  - [RelationCreateStorage](../R/RelationCreateStorage.md)
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md)
  - [index_build](../i/index_build.md)
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md)

## Notes and Other Information
- The function creates an xl_smgr_create structure containing the rlocator and forkNum before logging
- Uses RM_SMGR_ID resource manager for storage manager operations in WAL
- The XLR_SPECIAL_REL_UPDATE flag marks this as a special relation update that may affect system catalogs
- This logging is essential for crash recovery to ensure physical files exist when transactions are replayed
- Called only for persistent relations that require WAL logging