# EndPrepare

## Location
[src/backend/access/transam/twophase.c:1142-1263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1142-L1263)

## Overview
EndPrepare completes the two-phase commit state file preparation by finalizing the state data and writing it to the Write-Ahead Log (WAL).

## Definition

```c
void
EndPrepare(GlobalTransaction gxact)
```
## Detailed Description
EndPrepare finalizes the two-phase commit preparation process initiated by StartPrepare. It adds an end sentinel record to the 2PC records, updates the total length in the file header, handles replication origin information if present, validates the data size doesn't exceed limits, and writes the entire state data to WAL within a critical section. The function ensures proper checkpoint coordination, marks the transaction as prepared, and handles synchronous replication requirements.

## Parameters / Member Variables
- : GlobalTransaction structure representing the transaction being prepared, which will store prepare LSN information and be marked as prepared

## Dependencies
- Functions called/Symbols referenced:
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [MarkAsPrepared](../M/MarkAsPrepared.md)
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
  - [replorigin_session_advance](../r/replorigin_session_advance.md)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Uses DELAY_CHKPT_START flags to coordinate with checkpoint process to ensure state file is properly fsync'd
- Validates total data size against MaxAllocSize to prevent issues during recovery
- Handles replication origin metadata (LSN and timestamp) for logical replication scenarios
- Creates WAL record of type XLOG_XACT_PREPARE with XLOG_INCLUDE_ORIGIN flag
- Maintains critical section semantics around WAL writing and transaction state changes
- Sets MyLockedGxact to ensure proper cleanup if process crashes after preparation
- Waits for synchronous replication confirmation if required by configuration
- Cleans up the records data structure after successful completion

## Simplified Source

```c
void EndPrepare(GlobalTransaction gxact)
{
    TwoPhaseFileHeader *hdr;
    StateFileChunk *record;
    bool replorigin;

    // Add end sentinel to the 2PC records
    RegisterTwoPhaseRecord(TWOPHASE_RM_END_ID, 0, NULL, 0);

    // Fill in total length in the file header
    hdr = (TwoPhaseFileHeader *) records.head->data;
    Assert(hdr->magic == TWOPHASE_MAGIC);
    hdr->total_len = records.total_len + sizeof(pg_crc32c);

    // Handle replication origin information if present
    replorigin = (replorigin_session_origin != InvalidRepOriginId &&
                  replorigin_session_origin != DoNotReplicateId);

    if (replorigin)
    {
        hdr->origin_lsn = replorigin_session_origin_lsn;
        hdr->origin_timestamp = replorigin_session_origin_timestamp;
    }

    // Check size limits to ensure we can read this back later
    if (hdr->total_len > MaxAllocSize)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("two-phase state file maximum length exceeded")));

    // Write 2PC state data to WAL
    XLogEnsureRecordSpace(0, records.num_chunks);

    START_CRIT_SECTION();

    // Delay checkpoint start to ensure state file gets fsync'd
    Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0);
    MyProc->delayChkptFlags |= DELAY_CHKPT_START;

    // Register all record chunks with WAL
    XLogBeginInsert();
    for (record = records.head; record != NULL; record = record->next)
        XLogRegisterData(record->data, record->len);

    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    // Insert the PREPARE record and get LSN
    gxact->prepare_end_lsn = XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE);

    // Advance replication origin if applicable
    if (replorigin)
        replorigin_session_advance(replorigin_session_origin_lsn,
                                   gxact->prepare_end_lsn);

    XLogFlush(gxact->prepare_end_lsn);

    // Store start location for later commit/rollback operations
    gxact->prepare_start_lsn = ProcLastRecPtr;

    // Mark transaction as prepared (creates dummy ProcArray entry)
    MarkAsPrepared(gxact, false);

    // Allow checkpoints to proceed now
    MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

    // Remember we have this gxact locked
    MyLockedGxact = gxact;

    END_CRIT_SECTION();

    // Wait for synchronous replication if required
    SyncRepWaitForLSN(gxact->prepare_end_lsn, false);

    // Clean up records structure
    records.tail = records.head = NULL;
    records.num_chunks = 0;
}
```