# smgrimmedsync

## Location
[src/backend/storage/smgr/smgr.c:815-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L815-L832)

## Overview
Forces the specified relation to stable storage by synchronously forcing all previous writes down to disk, primarily used for building new relations like indexes where WAL logging is bypassed.

## Definition
void smgrimmedsync(SMgrRelation reln, ForkNumber forknum)

## Detailed Description
The smgrimmedsync function provides immediate synchronization of a relation to stable storage, bypassing the normal checkpoint-based sync mechanism. It's particularly useful for building completely new relations (such as indexes) where incremental WAL logging would be inefficient. Instead of logging each build step, the system can write completed pages with smgrwrite/smgrextend and then use smgrimmedsync to ensure durability before transaction commit.

This function is sufficient for crash recovery purposes as it effectively duplicates forcing a checkpoint for the completed relation. However, it's not sufficient for PITR (Point-In-Time Recovery) or replication purposes, which require proper WAL entries.

The function delegates to the appropriate storage manager's immediate sync implementation through the smgrsw function table, maintaining the storage manager abstraction.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation to be immediately synchronized
- : ForkNumber indicating which fork of the relation needs immediate synchronization

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw (storage manager switch table)
  - SMgrRelation (storage manager relation structure)
- Called from (representative examples):
  - [smgr_bulk_finish](smgr_bulk_finish.md) (in bulk_write.c at line 214)

## Notes and Other Information
- Preceding writes should specify skipFsync = true to avoid duplicative fsyncs
- [FlushRelationBuffers](../F/FlushRelationBuffers.md)() must be called first if there's any possibility of dirty buffers for the relation
- Most callers should use the bulk loading facility in bulk_write.c instead of calling this directly
- Sufficient for crash recovery but not for PITR or replication (requires WAL entries)
- Used primarily for index builds and other bulk relation creation operations
- Provides synchronous I/O completion, making it more expensive than deferred sync operations