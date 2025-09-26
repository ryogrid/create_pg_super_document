# RmgrData

## Location
[src/include/access/xlog_internal.h:349-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L349-L360)

## Overview
RmgrData is a method table structure that defines the interface for resource managers in PostgreSQL's WAL (Write-Ahead Log) system, providing function pointers for WAL record processing, description, and management operations.

## Definition

```c
typedef struct RmgrData
{
	const char *rm_name;
	void		(*rm_redo) (XLogReaderState *record);
	void		(*rm_desc) (StringInfo buf, XLogReaderState *record);
	const char *(*rm_identify) (uint8 info);
	void		(*rm_startup) (void);
	void		(*rm_cleanup) (void);
	void		(*rm_mask) (char *pagedata, BlockNumber blkno);
	void		(*rm_decode) (struct LogicalDecodingContext *ctx,
							  struct XLogRecordBuffer *buf);
} RmgrData;
```
## Detailed Description
RmgrData serves as the method table for resource managers in PostgreSQL's WAL system. Each resource manager type (such as heap, btree, hash, etc.) registers an RmgrData structure that defines how to handle WAL records specific to that resource manager. This structure must be kept in sync with the PG_RMGR definition in rmgr.c. The RmgrTable[] array is indexed by RmgrId values, and entries with NULL rm_name are considered invalid. This design enables a pluggable architecture where different storage components can register their own WAL processing logic.

## Parameters / Member Variables
- `*rm_name`: Name identifier for the resource manager (e.g., "Heap", "Btree")
- `*record)`: Function pointer for replaying/redoing WAL records during recovery
- `*record)`: Function pointer for generating human-readable descriptions of WAL records (used by tools like pg_waldump)
- `info)`: Function pointer that returns a name for the record type based on xl_info field (without reference to rmid)
- `(void)`: Function pointer called during WAL recovery startup for this resource manager
- `(void)`: Function pointer called during WAL recovery cleanup for this resource manager
- `blkno)`: Function pointer that masks out bits in pages that shouldn't be flagged by wal_consistency_checking
- `*buf)`: Function pointer for logical decoding of WAL records for replication purposes
## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderState](../X/XLogReaderState.md) (used in rm_redo and rm_desc)
  - StringInfo (used in rm_desc)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (used in rm_decode)
  - [XLogRecordBuffer](../X/XLogRecordBuffer.md) (used in rm_decode)
- Called from (representative examples):
  - [RegisterCustomRmgr](RegisterCustomRmgr.md)
  - [xlog_outdesc](../x/xlog_outdesc.md)
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md)
  - [RmgrIdExists](RmgrIdExists.md)

## Notes and Other Information
- This structure enables the extensible resource manager architecture in PostgreSQL, allowing different storage components to plug into the WAL system
- The rm_identify method should return names like "VACUUM" for XLOG_BTREE_VACUUM, providing context-independent record type identification
- The rm_desc method can provide additional detail for records, such as information about the last block affected
- The rm_mask function is crucial for WAL consistency checking, ensuring that irrelevant page bits don't cause false positives during verification
- Custom resource managers can be registered using this interface, as demonstrated in the test_custom_rmgrs module
- The structure is defined in xlog_internal.h, making it part of the internal WAL implementation interface