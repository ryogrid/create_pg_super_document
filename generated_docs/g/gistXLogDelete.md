# gistXLogDelete

## Location
src/backend/access/gist/gistxlog.c: 670 - 695

## Overview
gistXLogDelete creates a Write-Ahead Log (WAL) record for deleting leaf index tuples marked as DEAD during new tuple insertion in GiST indexes, with special handling for standby query conflicts.

## Definition
XLogRecPtr gistXLogDelete(Buffer buffer, OffsetNumber *todelete, int ntodelete, TransactionId snapshotConflictHorizon, Relation heaprel)

## Detailed Description
gistXLogDelete is specialized for recording the deletion of leaf index tuples that have been marked as DEAD during new tuple insertion operations in GiST indexes. While similar operations might seem covered by gistXLogUpdate, this function provides distinct handling for deletion scenarios that can conflict with standby queries in streaming replication environments.

The function creates WAL records with conflict resolution information, including a snapshot conflict horizon that helps standby servers determine when it's safe to apply the deletion without interfering with running read-only queries. This is crucial for maintaining consistency in hot standby configurations.

The function also tracks whether the relation is accessible in logical decoding, which affects how the WAL record is processed during logical replication.

## Parameters / Member Variables
- buffer: The target buffer containing the GiST index page with tuples to delete
- todelete: Array of offset numbers identifying the tuples to be deleted
- ntodelete: Number of tuples to delete (length of todelete array)
- snapshotConflictHorizon: Transaction ID representing the conflict horizon for standby queries
- heaprel: The heap relation associated with this index operation

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsAccessibleInLogicalDecoding
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - SizeOfGistxlogDelete
  - REGBUF_STANDARD
  - RM_GIST_ID
  - XLOG_GIST_DELETE
- Called from (representative examples):
  - [gistprunepage](gistprunepage.md)

## Notes and Other Information
- The target-offsets array is always stored in the WAL record regardless of whether the whole buffer is logged, enabling standby servers to locate the snapshotConflictHorizon
- Uses WAL record type XLOG_GIST_DELETE with resource manager RM_GIST_ID
- The isCatalogRel flag indicates if the relation is accessible during logical decoding
- Returns an XLogRecPtr representing the LSN of the inserted WAL record
- Provides better conflict resolution for hot standby scenarios compared to generic page updates