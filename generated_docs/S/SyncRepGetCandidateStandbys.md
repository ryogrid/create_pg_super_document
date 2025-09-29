# SyncRepGetCandidateStandbys

## Location
[src/backend/replication/syncrep.c:754-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L754-L832)

## Overview
Returns data about walsenders that are candidates to be synchronous standbys, collecting and filtering active walsenders based on their state and synchronous priority.

## Definition
```c
int SyncRepGetCandidateStandbys(SyncRepStandbyData **standbys)
```

## Detailed Description
This function scans all available walsender processes to identify those that are eligible to be synchronous standbys. It allocates memory for a result array and populates it with detailed information about each candidate standby, including their WAL positions and priority levels. The function applies several filtering criteria to ensure only valid candidates are included:

- The walsender must be active (pid != 0)
- Must be in STREAMING or STOPPING state
- Must have a non-zero synchronous standby priority
- Must have a valid flush position

In priority mode, if there are more candidates than needed (num_sync), the function sorts them by priority and returns only the highest priority ones. In quorum mode, all candidates are returned.

## Parameters / Member Variables
- `standbys`: Output parameter - pointer to a palloc'd array of SyncRepStandbyData structs containing information about candidate standbys

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - SpinLockAcquire/SpinLockRelease (shared memory synchronization)
  - XLogRecPtrIsInvalid (WAL position validation)
  - qsort (sorting in priority mode)
  - [standby_priority_comparator](../s/standby_priority_comparator.md) (comparison function for sorting)
- Called from (representative examples):
  - [SyncRepGetSyncRecPtr](SyncRepGetSyncRecPtr.md) (src/backend/replication/syncrep.c:604)
  - PG_STAT_GET_WAL_SENDERS_COLS (src/backend/replication/walsender.c:3919)

## Notes and Other Information
- Returns the number of valid candidate standbys found
- The caller is responsible for freeing the allocated memory
- Behavior differs between priority and quorum synchronous replication modes
- Uses volatile pointers when accessing shared memory to prevent compiler optimizations
- Quick exit optimization when SyncRepConfig is NULL (sync replication not configured)

## Simplified Source

```c
int SyncRepGetCandidateStandbys(SyncRepStandbyData **standbys) {
    int i, n;

    // Allocate result array for all possible walsenders
    *standbys = (SyncRepStandbyData *)
        palloc(max_wal_senders * sizeof(SyncRepStandbyData));

    // Quick exit if sync replication is not configured
    if (SyncRepConfig == NULL)
        return 0;

    // Collect candidate standbys from shared memory
    n = 0;
    for (i = 0; i < max_wal_senders; i++) {
        volatile WalSnd *walsnd = &WalSndCtl->walsnds[i];
        SyncRepStandbyData *stby = *standbys + n;
        WalSndState state;

        // Copy data under spinlock protection
        SpinLockAcquire(&walsnd->mutex);
        stby->pid = walsnd->pid;
        state = walsnd->state;
        stby->write = walsnd->write;
        stby->flush = walsnd->flush;
        stby->apply = walsnd->apply;
        stby->sync_standby_priority = walsnd->sync_standby_priority;
        SpinLockRelease(&walsnd->mutex);

        // Apply filtering criteria
        if (stby->pid == 0) continue;                          // Must be active
        if (state != WALSNDSTATE_STREAMING &&                 // Must be streaming
            state != WALSNDSTATE_STOPPING) continue;          // or stopping
        if (stby->sync_standby_priority == 0) continue;       // Must be synchronous
        if (XLogRecPtrIsInvalid(stby->flush)) continue;       // Must have valid flush position

        // Valid candidate found
        stby->walsnd_index = i;
        stby->is_me = (walsnd == MyWalSnd);
        n++;
    }

    // In priority mode, limit to num_sync highest priority standbys
    if (SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY &&
        n > SyncRepConfig->num_sync) {
        // Sort by priority and keep only the top ones
        qsort(*standbys, n, sizeof(SyncRepStandbyData),
              standby_priority_comparator);
        n = SyncRepConfig->num_sync;
    }

    return n;
}
```