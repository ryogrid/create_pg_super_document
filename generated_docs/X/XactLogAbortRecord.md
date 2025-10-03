# XactLogAbortRecord

## Location
[src/backend/access/transam/xact.c:5924-6067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5924-L6067)

## Overview
Creates and writes a WAL (Write-Ahead Log) record for transaction abort operations, supporting both plain transaction aborts and two-phase commit transaction aborts.

## Definition

```c
XLogRecPtr
XactLogAbortRecord(TimestampTz abort_time,
				   int nsubxacts, TransactionId *subxacts,
				   int nrels, RelFileLocator *rels,
				   int ndroppedstats, xl_xact_stats_item *droppedstats,
				   int xactflags, TransactionId twophase_xid,
				   const char *twophase_gid)
```
## Detailed Description
This function constructs and logs a comprehensive abort record to the Write-Ahead Log for transaction rollback operations. It handles both regular transaction aborts and two-phase commit prepared transaction aborts. The function collects various transaction-related metadata including sub-transactions, file relationships, dropped statistics, access exclusive locks, replication origin information, and two-phase commit details, then packages them into a structured WAL record for crash recovery and replication purposes.

## Parameters / Member Variables
- `abort_time`: Timestamp when the transaction abort occurred
- `nsubxacts`: Number of sub-transactions involved in this abort
- `*subxacts`: Array of sub-transaction IDs that are being aborted
- `nrels`: Number of relation file locators affected by this transaction
- `*rels`: Array of RelFileLocator structures for relations modified by the transaction
- `ndroppedstats`: Number of statistics items dropped during this transaction
- `*droppedstats`: Array of xl_xact_stats_item structures for dropped statistics
- `xactflags`: Transaction flags indicating special properties (e.g., XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK)
- `twophase_xid`: Transaction ID for two-phase commit operations (InvalidTransactionId for regular aborts)
- `*twophase_gid`: Global identifier string for two-phase transactions (NULL for regular aborts)
## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](XLogBeginInsert.md)
  - [XLogRegisterData](XLogRegisterData.md)
  - [XLogSetRecordFlags](XLogSetRecordFlags.md)
  - [XLogInsert](XLogInsert.md)
  - XLogLogicalInfoActive
  - unconstify
- Called from (representative examples):
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)
  - [RecordTransactionAbortPrepared](../R/RecordTransactionAbortPrepared.md)

## Notes and Other Information
The function differentiates between regular transaction aborts (XLOG_XACT_ABORT) and prepared transaction aborts (XLOG_XACT_ABORT_PREPARED) based on the validity of twophase_xid. It conditionally includes various information blocks in the WAL record using xinfo flags, ensuring efficient storage by only including relevant data. The function operates within a critical section and includes replication origin information when applicable for proper logical replication support.

## Simplified Source

```c
XLogRecPtr XactLogAbortRecord(TimestampTz abort_time,
                              int nsubxacts, TransactionId *subxacts,
                              int nrels, RelFileLocator *rels,
                              int ndroppedstats, xl_xact_stats_item *droppedstats,
                              int xactflags, TransactionId twophase_xid,
                              const char *twophase_gid)
{
    xl_xact_abort xlrec;
    xl_xact_xinfo xl_xinfo;
    uint8 info;

    Assert(CritSectionCount > 0);

    xl_xinfo.xinfo = 0;

    // Determine abort type: regular or two-phase
    if (!TransactionIdIsValid(twophase_xid))
        info = XLOG_XACT_ABORT;
    else
        info = XLOG_XACT_ABORT_PREPARED;

    // Set basic abort record data
    xlrec.xact_time = abort_time;

    // Conditionally add information blocks based on transaction properties
    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;

    if (nsubxacts > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;

    if (nrels > 0) {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        info |= XLR_SPECIAL_REL_UPDATE;
    }

    if (ndroppedstats > 0)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_DROPPED_STATS;

    if (TransactionIdIsValid(twophase_xid)) {
        xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
        if (XLogLogicalInfoActive()) {
            xl_xinfo.xinfo |= XACT_XINFO_HAS_GID;
            xl_xinfo.xinfo |= XACT_XINFO_HAS_DBINFO;
        }
    }

    if (replorigin_session_origin != InvalidRepOriginId)
        xl_xinfo.xinfo |= XACT_XINFO_HAS_ORIGIN;

    if (xl_xinfo.xinfo != 0)
        info |= XLOG_XACT_HAS_INFO;

    // Register all data blocks for WAL record
    XLogBeginInsert();
    XLogRegisterData((char *) (&xlrec), MinSizeOfXactAbort);

    // Add conditional data blocks based on xinfo flags
    if (xl_xinfo.xinfo != 0)
        XLogRegisterData((char *) (&xl_xinfo), sizeof(xl_xinfo));

    // [Additional XLogRegisterData calls for each optional section...]

    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
    return XLogInsert(RM_XACT_ID, info);
}
```

**Simplified Logic:**
1. Determine if this is a regular or two-phase abort
2. Set up basic abort record with timestamp
3. Examine transaction properties and set appropriate xinfo flags
4. Register base abort record data with WAL system
5. Conditionally register additional data blocks based on xinfo flags
6. Insert the complete WAL record and return its LSN

**Key Points:**
- Handles both regular and prepared transaction aborts
- Uses conditional xinfo flags to include only relevant data
- Operates within critical section for atomicity
- Supports replication origin tracking for logical replication
- Returns WAL LSN for the inserted abort record