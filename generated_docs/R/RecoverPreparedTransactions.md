# RecoverPreparedTransactions

## Location
[src/backend/access/transam/twophase.c:2074-2176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2074-L2176)

## Overview
RecoverPreparedTransactions fully restores prepared transactions at the end of recovery, reacquiring locks and reconstructing complete transaction state before normal operations resume.

## Definition
```c
void RecoverPreparedTransactions(void)
```

## Detailed Description
RecoverPreparedTransactions is called at the end of recovery, just before backends are allowed to write WAL again. It performs complete restoration of prepared transactions by rebuilding all necessary transaction state including locks, subtransaction hierarchies, and resource manager state.

The function operates through several critical phases:

1. **Transaction State Reconstruction**: Processes each prepared transaction's buffer data and reconstructs the TwoPhaseFileHeader and associated metadata
2. **Subtransaction Hierarchy**: Rebuilds pg_subtrans state by linking all subtransactions directly to their top-level XID (flattening any original complex hierarchy)
3. **GXACT Recreation**: Uses MarkAsPreparingGuts to recreate the GlobalTransaction and associated PGPROC structures
4. **Lock Recovery**: Calls ProcessRecords with twophase_recover_callbacks to restore all locks and resource manager state
5. **Hot Standby Cleanup**: Releases standby-held locks if running in hot standby mode
6. **State Finalization**: Marks transactions as fully prepared and clears recovery flags

This comprehensive recovery ensures that prepared transactions are completely functional and ready for commit or abort operations when normal database operations resume.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [MarkAsPreparingGuts](../M/MarkAsPreparingGuts.md)
  - [GXactLoadSubxactData](../G/GXactLoadSubxactData.md)
  - [MarkAsPrepared](../M/MarkAsPrepared.md)
  - [ProcessRecords](../P/ProcessRecords.md)
  - [StandbyReleaseLockTree](../S/StandbyReleaseLockTree.md)
  - [PostPrepare_Twophase](../P/PostPrepare_Twophase.md)
  - TransactionIdEquals
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Run at the end of recovery before backends can write WAL
- Flattens subtransaction hierarchies by linking all subtxns directly to top-level XID
- Uses twophase_recover_callbacks for resource manager state recovery
- Handles hot standby lock cleanup to prevent lock accumulation
- Logs each recovered prepared transaction for monitoring purposes
- Essential for maintaining prepared transaction semantics across restart cycles
- Calls PostPrepare_Twophase to clean up MyLockedGxact like normal operation

## Simplified Source

```c
// Simplified version of RecoverPreparedTransactions
void RecoverPreparedTransactions(void) {
    int i;

    // Lock the two-phase state for exclusive access
    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

    // Process each prepared transaction
    for (i = 0; i < TwoPhaseState->numPrepXacts; i++) {
        TransactionId xid;
        char *buf;
        GlobalTransaction gxact = TwoPhaseState->prepXacts[i];
        TwoPhaseFileHeader *hdr;
        TransactionId *subxids;
        const char *gid;

        xid = gxact->xid;

        // Load transaction buffer data from shared memory
        buf = ProcessTwoPhaseBuffer(xid, gxact->prepare_start_lsn,
                                   gxact->ondisk, true, false);
        if (buf == NULL)
            continue;

        ereport(LOG, (errmsg("recovering prepared transaction %u from shared memory", xid)));

        // Parse the transaction data buffer
        hdr = (TwoPhaseFileHeader *) buf;
        char *bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeader));
        gid = (const char *) bufptr;
        bufptr += MAXALIGN(hdr->gidlen);
        subxids = (TransactionId *) bufptr;
        // Skip to end of buffer data...

        // Recreate the global transaction state
        MarkAsPreparingGuts(gxact, xid, gid, hdr->prepared_at,
                           hdr->owner, hdr->database);
        gxact->inredo = false;

        // Load subtransaction data and mark as prepared
        GXactLoadSubxactData(gxact, hdr->nsubxacts, subxids);
        MarkAsPrepared(gxact, true);

        LWLockRelease(TwoPhaseStateLock);

        // Recover locks and other resource manager state
        ProcessRecords(bufptr, xid, twophase_recover_callbacks);

        // Release standby locks if in hot standby mode
        if (InHotStandby)
            StandbyReleaseLockTree(xid, hdr->nsubxacts, subxids);

        // Clean up transaction state
        PostPrepare_Twophase();
        pfree(buf);

        // Reacquire lock for next iteration
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
    }

    LWLockRelease(TwoPhaseStateLock);
}
```

Key simplifications made:
- Removed complex buffer pointer arithmetic and consolidated buffer parsing
- Simplified comments to focus on main algorithm steps
- Abstracted detailed memory layout calculations
- Consolidated variable declarations for clarity
- Removed detailed assertions and error handling paths
- Focused on the core recovery workflow: load → recreate → recover → cleanup