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

## Simplified Source

```c
XLogRecPtr XactLogCommitRecord(TimestampTz commit_time,
                              int nsubxacts, TransactionId *subxacts,
                              int nrels, RelFileLocator *rels,
                              int ndroppedstats, xl_xact_stats_item *droppedstats,
                              int nmsgs, SharedInvalidationMessage *msgs,
                              bool relcacheInval, int xactflags,
                              TransactionId twophase_xid, const char *twophase_gid) {
    xl_xact_commit xlrec;
    xl_xact_xinfo xl_xinfo;
    uint8 info;

    // Initialize structures
    xl_xinfo.xinfo = 0;

    // Determine commit type: regular or two-phase commit
    if (!TransactionIdIsValid(twophase_xid))
        info = XLOG_XACT_COMMIT;
    else
        info = XLOG_XACT_COMMIT_PREPARED;

    // Set basic commit time
    xlrec.xact_time = commit_time;

    // Build transaction info flags based on what data we have
    if (relcacheInval)
        xl_xinfo.xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    if (forceSyncCommit)
        xl_xinfo.xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
    if (synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
        xl_xinfo.xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;

    // Set flags for optional data sections
    if (nmsgs > 0 || XLogLogicalInfoActive())
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DBINFO;
    if (nsubxacts > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
    if (nrels > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
    if (ndroppedstats > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
    if (nmsgs > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_INVALS;
    if (TransactionIdIsValid(twophase_xid))
        xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
    if (replorigin_session_origin != InvalidRepOriginId)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_ORIGIN;

    if (xl_xinfo.xinfo != 0)
        info |= XLOG_XACT_HAS_INFO;

    // Build the WAL record by registering all data sections
    XLogBeginInsert();

    // Register the basic commit record
    XLogRegisterData((char *) (&xlrec), sizeof(xl_xact_commit));

    // Register optional sections based on flags set above
    if (xl_xinfo.xinfo != 0)
        XLogRegisterData((char *) (&xl_xinfo.xinfo), sizeof(xl_xinfo.xinfo));

    // Register database info, subtransactions, relations, etc. based on flags
    // ... (register various optional data sections as needed)

    // Set record flags and insert into WAL
    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
    return XLogInsert(RM_XACT_ID, info);
}
```