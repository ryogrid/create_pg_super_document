# SnapBuildCommitTxn

## Location
[src/backend/replication/logical/snapbuild.c:1078-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1078-L1243)

## Overview
Handles all necessary processing when a transaction commits in the logical replication snapshot building context, managing snapshot state transitions, catalog change tracking, and timeline visibility.

## Definition

```c
void
SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid,
				   int nsubxacts, TransactionId *subxacts, uint32 xinfo)
```
## Detailed Description
SnapBuildCommitTxn is a central function in PostgreSQL's logical replication snapshot building mechanism. It processes transaction commits and determines their impact on the evolving snapshot state. The function handles multiple scenarios based on the builder's current state:

1. **Early State Filtering**: Transactions preceding the BUILDING_SNAPSHOT phase are ignored as they won't be decoded or included in snapshots.

2. **Catalog Change Detection**: Uses SnapBuildXidHasCatalogChanges to identify transactions that modified system catalogs, which require special handling for consistent snapshot building.

3. **Subtransaction Processing**: Iterates through all subtransactions, adding catalog-modifying ones to the committed transaction set and tracking their visibility requirements.

4. **Timeline Management**: Manages timetravel requirements for maintaining historical visibility and adjusts the builder's xmax to encompass all relevant committed transactions.

5. **Snapshot Distribution**: When necessary, builds and distributes new snapshots to maintain consistency across the replication system.

The function ensures that only transactions relevant to logical replication are tracked while maintaining proper visibility semantics for historical snapshot reconstruction.

## Parameters / Member Variables
- `*builder`: The SnapBuild context containing the current snapshot building state
- `lsn`: Log sequence number where the commit was logged
- `xid`: Transaction ID of the committing transaction
- `nsubxacts`: Number of subtransactions in the subxacts array
- `*subxacts`: Array of subtransaction IDs that are part of this commit
- `xinfo`: Additional transaction information flags
## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [SnapBuildXidHasCatalogChanges](SnapBuildXidHasCatalogChanges.md)
  - [SnapBuildAddCommittedTxn](SnapBuildAddCommittedTxn.md)
  - NormalTransactionIdFollows
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - TransactionIdAdvance
  - [SnapBuildSnapDecRefcount](SnapBuildSnapDecRefcount.md)
  - [SnapBuildBuildSnapshot](SnapBuildBuildSnapshot.md)
  - [ReorderBufferXidHasBaseSnapshot](../R/ReorderBufferXidHasBaseSnapshot.md)
  - [SnapBuildSnapIncRefcount](SnapBuildSnapIncRefcount.md)
  - [ReorderBufferSetBaseSnapshot](../R/ReorderBufferSetBaseSnapshot.md)
  - [SnapBuildDistributeSnapshotAndInval](SnapBuildDistributeSnapshotAndInval.md)
- Called from (representative examples):
  - [DecodeCommit](../D/DecodeCommit.md)

## Notes and Other Information
- Critical for maintaining snapshot consistency during logical replication setup
- Handles complex state transitions in the snapshot building process (START → BUILDING_SNAPSHOT → CONSISTENT → FULL_SNAPSHOT)
- Must carefully track catalog-modifying transactions to ensure schema changes are properly reflected
- Manages reference counting for snapshot objects to prevent memory leaks
- Uses debugging output levels (DEBUG1, DEBUG2) for transaction tracking diagnostics
- The function's behavior changes significantly based on the builder->state, making it a state-machine-driven operation

## Simplified Source

```c
void
SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid,
                   int nsubxacts, TransactionId *subxacts, uint32 xinfo)
{
    bool needs_snapshot = false;
    bool needs_timetravel = false;
    bool sub_needs_timetravel = false;
    TransactionId xmax = xid;

    // Skip transactions before we start building snapshots
    if (builder->state == SNAPBUILD_START ||
        (builder->state == SNAPBUILD_BUILDING_SNAPSHOT &&
         TransactionIdPrecedes(xid, builder->next_phase_at)))
    {
        if (builder->start_decoding_at <= lsn)
            builder->start_decoding_at = lsn + 1;
        return;
    }

    // Pre-consistent state: update decoding start point and check for full snapshot building
    if (builder->state < SNAPBUILD_CONSISTENT)
    {
        if (builder->start_decoding_at <= lsn)
            builder->start_decoding_at = lsn + 1;

        if (builder->building_full_snapshot)
            needs_timetravel = true;
    }

    // Process all subtransactions
    for (int nxact = 0; nxact < nsubxacts; nxact++)
    {
        TransactionId subxid = subxacts[nxact];

        if (SnapBuildXidHasCatalogChanges(builder, subxid, xinfo))
        {
            // Catalog-modifying subtransaction needs tracking
            sub_needs_timetravel = true;
            needs_snapshot = true;
            SnapBuildAddCommittedTxn(builder, subxid);

            if (NormalTransactionIdFollows(subxid, xmax))
                xmax = subxid;
        }
        else if (needs_timetravel)
        {
            // Track subtransaction for timetravel even without catalog changes
            SnapBuildAddCommittedTxn(builder, subxid);
            if (NormalTransactionIdFollows(subxid, xmax))
                xmax = subxid;
        }
    }

    // Process top-level transaction
    if (SnapBuildXidHasCatalogChanges(builder, xid, xinfo))
    {
        needs_snapshot = true;
        needs_timetravel = true;
        SnapBuildAddCommittedTxn(builder, xid);
    }
    else if (sub_needs_timetravel || needs_timetravel)
    {
        needs_timetravel = true;
        SnapBuildAddCommittedTxn(builder, xid);
    }

    if (!needs_timetravel)
        builder->committed.includes_all_transactions = false;

    // Update xmax for catalog-modifying transactions
    if (needs_timetravel &&
        (!TransactionIdIsValid(builder->xmax) ||
         TransactionIdFollowsOrEquals(xmax, builder->xmax)))
    {
        builder->xmax = xmax;
        TransactionIdAdvance(builder->xmax);
    }

    // Build and distribute snapshot if needed
    if (needs_snapshot && builder->state >= SNAPBUILD_FULL_SNAPSHOT)
    {
        // Replace old snapshot with new one
        if (builder->snapshot)
            SnapBuildSnapDecRefcount(builder->snapshot);

        builder->snapshot = SnapBuildBuildSnapshot(builder);

        // Set base snapshot if needed
        if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid))
        {
            SnapBuildSnapIncRefcount(builder->snapshot);
            ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn, builder->snapshot);
        }

        SnapBuildSnapIncRefcount(builder->snapshot);
        SnapBuildDistributeSnapshotAndInval(builder, lsn, xid);
    }
}
```