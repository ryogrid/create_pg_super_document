# RelationCopyStorage

## Location
[src/backend/catalog/storage.c:477-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L477-L556)

## Overview
RelationCopyStorage copies data from one relation storage to another, block by block, handling WAL logging and data validation during the copy process.

## Definition
```c
void RelationCopyStorage(SMgrRelation src, SMgrRelation dst,
                        ForkNumber forkNum, char relpersistence)
```

## Detailed Description
RelationCopyStorage performs a complete block-by-block copy of relation data from a source storage manager relation to a destination. This is a fundamental operation used during relation restructuring operations like table rewrites and index rebuilds.

Key aspects of the operation:
1. **WAL Logging Decision**: Determines whether to use WAL logging based on persistence type and XLog requirements
2. **Bulk Writing**: Uses the bulk write API for efficient data copying with proper WAL integration  
3. **Data Validation**: Validates each page during the copy process using PageIsVerifiedExtended
4. **Error Handling**: Provides detailed error reporting for corrupted pages encountered during copying
5. **Special Init Fork Handling**: Treats init forks of unlogged relations specially, requiring WAL logging

The function handles different relation persistence types:
- PERMANENT relations: Always WAL-logged when XLog is needed
- UNLOGGED relations: Generally not WAL-logged, except for init forks
- INIT forks of unlogged relations: Treated like permanent relations for WAL purposes

## Parameters / Member Variables
- `src`: Source SMgrRelation to copy data from
- `dst`: Destination SMgrRelation to copy data to  
- `forkNum`: The fork number to copy (MAIN_FORKNUM, FSM_FORKNUM, etc.)
- `relpersistence`: Persistence characteristic of the relation (PERMANENT, UNLOGGED, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_start_smgr](../s/smgr_bulk_start_smgr.md)
  - [smgrnblocks](../s/smgrnblocks.md)
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md)
  - [smgrread](../s/smgrread.md)
  - [PageIsVerifiedExtended](../P/PageIsVerifiedExtended.md)
  - relpathbackend
  - [smgr_bulk_write](../s/smgr_bulk_write.md)
  - [smgr_bulk_finish](../s/smgr_bulk_finish.md)
  - XLogIsNeeded
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md)
  - [index_copy_data](../i/index_copy_data.md)

## Notes and Other Information
- Requires that no dirty data exists in shared buffers - callers should flush using FlushRelationBuffers() if needed
- Uses bulk write operations for efficiency and proper WAL integration
- Performs page validation on every copied block to detect corruption
- Supports interruption via CHECK_FOR_INTERRUPTS() for long-running operations
- Safe to call with RelationGetSmgr(rel) patterns since it only uses smgr and WAL operations
- Init forks of unlogged relations receive special treatment and are WAL-logged like permanent relations