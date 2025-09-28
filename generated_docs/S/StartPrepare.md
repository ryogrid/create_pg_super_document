# StartPrepare

## Location
[src/backend/access/transam/twophase.c:1049-1141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1049-L1141)

## Overview
StartPrepare initializes the two-phase commit state file preparation process by creating data structures and inserting the 2PC file header record.

## Definition

```c
void
StartPrepare(GlobalTransaction gxact)
```
## Detailed Description
StartPrepare begins the process of preparing a two-phase commit state file for a given global transaction. It initializes the linked list data structure used to accumulate state data, creates and populates the two-phase file header with transaction metadata, and saves initial state information including subtransactions, file deletions, statistics, and cache invalidation messages. The function sets up the foundation for the state file that will be completed by EndPrepare.

## Parameters / Member Variables
- : GlobalTransaction structure containing the transaction to be prepared, including transaction ID, GID, owner, and timing information

## Dependencies
- Functions called/Symbols referenced:
  - GetPGProcByNumber
  - [xactGetCommittedChildren](../x/xactGetCommittedChildren.md)
  - [smgrGetPendingDeletes](../s/smgrGetPendingDeletes.md)
  - [pgstat_get_transactional_drops](../p/pgstat_get_transactional_drops.md)
  - [xactGetCommittedInvalidationMessages](../x/xactGetCommittedInvalidationMessages.md)
  - [save_state_data](../s/save_state_data.md)
  - [GXactLoadSubxactData](../G/GXactLoadSubxactData.md)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Initializes the global  data structure used throughout the 2PC preparation process
- Creates TwoPhaseFileHeader with magic number TWOPHASE_MAGIC for file format identification
- Handles various transaction artifacts: subtransactions, file deletions (commit/abort), statistics drops, and cache invalidation messages
- The total_len field in the header is filled in later by EndPrepare
- Memory allocation uses palloc/palloc0 for PostgreSQL memory management
- Part of the two-phase commit protocol implementation in PostgreSQL

## Simplified Source

```c
// Simplified version of StartPrepare
void StartPrepare(GlobalTransaction gxact) {
    PGPROC *proc = GetPGProcByNumber(gxact->pgprocno);
    TransactionId xid = gxact->xid;
    TwoPhaseFileHeader hdr;
    TransactionId *children;
    RelFileLocator *commitrels, *abortrels;
    xl_xact_stats_item *abortstats = NULL, *commitstats = NULL;
    SharedInvalidationMessage *invalmsgs;

    // Initialize state file data structure
    records.head = palloc0(sizeof(StateFileChunk));
    records.head->len = 0;
    records.head->next = NULL;
    records.bytes_free = Max(sizeof(TwoPhaseFileHeader), 512);
    records.head->data = palloc(records.bytes_free);
    records.tail = records.head;
    records.num_chunks = 1;
    records.total_len = 0;

    // Create and populate two-phase file header
    hdr.magic = TWOPHASE_MAGIC;
    hdr.total_len = 0;  // EndPrepare will fill this
    hdr.xid = xid;
    hdr.database = proc->databaseId;
    hdr.prepared_at = gxact->prepared_at;
    hdr.owner = gxact->owner;

    // Gather transaction metadata
    hdr.nsubxacts = xactGetCommittedChildren(&children);
    hdr.ncommitrels = smgrGetPendingDeletes(true, &commitrels);
    hdr.nabortrels = smgrGetPendingDeletes(false, &abortrels);
    hdr.ncommitstats = pgstat_get_transactional_drops(true, &commitstats);
    hdr.nabortstats = pgstat_get_transactional_drops(false, &abortstats);
    hdr.ninvalmsgs = xactGetCommittedInvalidationMessages(&invalmsgs, &hdr.initfileinval);
    hdr.gidlen = strlen(gxact->gid) + 1;

    // Save header and GID
    save_state_data(&hdr, sizeof(TwoPhaseFileHeader));
    save_state_data(gxact->gid, hdr.gidlen);

    // Save transaction artifacts if present
    if (hdr.nsubxacts > 0) {
        save_state_data(children, hdr.nsubxacts * sizeof(TransactionId));
        GXactLoadSubxactData(gxact, hdr.nsubxacts, children);
    }
    if (hdr.ncommitrels > 0) {
        save_state_data(commitrels, hdr.ncommitrels * sizeof(RelFileLocator));
        pfree(commitrels);
    }
    if (hdr.nabortrels > 0) {
        save_state_data(abortrels, hdr.nabortrels * sizeof(RelFileLocator));
        pfree(abortrels);
    }
    if (hdr.ncommitstats > 0) {
        save_state_data(commitstats, hdr.ncommitstats * sizeof(xl_xact_stats_item));
        pfree(commitstats);
    }
    if (hdr.nabortstats > 0) {
        save_state_data(abortstats, hdr.nabortstats * sizeof(xl_xact_stats_item));
        pfree(abortstats);
    }
    if (hdr.ninvalmsgs > 0) {
        save_state_data(invalmsgs, hdr.ninvalmsgs * sizeof(SharedInvalidationMessage));
        pfree(invalmsgs);
    }
}
```

Key simplifications made:
- Preserved the essential two-phase commit preparation logic
- Maintained the data structure initialization and header creation
- Kept the transaction metadata gathering and state saving
- Focused on the core 2PC state file preparation process
- Retained memory management for allocated arrays