# XactLogCommitRecord

## Location
[src/backend/access/transam/xact.c:5752-5923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5752-L5923)

## Overview
A comprehensive function that creates and writes transaction commit records to PostgreSQL's Write-Ahead Log (WAL), supporting both regular and two-phase commit transactions with extensive metadata.

## Definition

```c
XLogRecPtr
XactLogCommitRecord(TimestampTz commit_time,
					int nsubxacts, TransactionId *subxacts,
					int nrels, RelFileLocator *rels,
					int ndroppedstats, xl_xact_stats_item *droppedstats,
					int nmsgs, SharedInvalidationMessage *msgs,
					bool relcacheInval,
					int xactflags, TransactionId twophase_xid,
					const char *twophase_gid)
```
## Detailed Description
XactLogCommitRecord is a critical function in PostgreSQL's transaction logging system that creates comprehensive WAL records for transaction commits. It handles both regular commits and two-phase commit prepared transactions, determining the record type based on whether a valid two-phase XID is provided. The function meticulously collects and organizes various types of transaction metadata including sub-transactions, relation file changes, dropped statistics, invalidation messages, and replication origin information. It constructs a complex WAL record structure with conditional components based on the transaction's characteristics, ensuring that all necessary information for transaction recovery and replication is properly logged. The function uses PostgreSQL's WAL insertion API to register multiple data segments in the proper order, creating a complete record that can be used for crash recovery, replication, and logical decoding.

## Parameters / Member Variables
- : The timestamp when the transaction was committed
- : Number of committed sub-transactions
- : Array of sub-transaction IDs that were committed
- : Number of relations affected by the transaction
- : Array of RelFileLocator structures for relations that need special handling
- : Number of dropped statistics items
- : Array of dropped statistics information
- : Number of shared invalidation messages
- : Array of SharedInvalidationMessage structures for cache invalidation
- : Boolean indicating if relation cache invalidation is needed
- : Transaction flags including access exclusive lock information
- : Transaction ID for two-phase commit (InvalidTransactionId for regular commits)
- : Global identifier string for two-phase transactions

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - XLogLogicalInfoActive
  - [XLogBeginInsert](XLogBeginInsert.md)
  - [XLogRegisterData](XLogRegisterData.md)
  - [XLogSetRecordFlags](XLogSetRecordFlags.md)
  - [XLogInsert](XLogInsert.md)
  - unconstify
  - Various WAL record structure types (xl_xact_commit, xl_xact_xinfo, etc.)
  - Multiple XACT_* constants and flags
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionCommitPrepared](../R/RecordTransactionCommitPrepared.md) (in two-phase commit)

## Notes and Other Information
- Returns an XLogRecPtr representing the WAL location of the inserted commit record
- Must be called within a critical section (Assert(CritSectionCount > 0))
- Supports extensive metadata including replication origin information, logical decoding data, and synchronous commit feedback requests
- Uses conditional WAL record components to optimize space usage based on transaction characteristics
- Handles both XLOG_XACT_COMMIT and XLOG_XACT_COMMIT_PREPARED record types
- Critical for crash recovery, streaming replication, and logical replication functionality
- The function carefully constructs variable-length WAL records with proper ordering of components
- Includes support for filtering by transaction origin for selective replication