# pgstat_reset_matching_entries

## Location
[src/backend/utils/activity/pgstat_shmem.c:1029-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L1029-L1058)

## Overview
This function scans through the shared hashtable of PostgreSQL statistics, resetting statistics entries that are approved by a provided callback function.

## Definition

```c
void
pgstat_reset_matching_entries(bool (*do_reset) (PgStatShared_HashEntry *, Datum),
							  Datum match_data, TimestampTz ts)
```
## Detailed Description
The function iterates through all entries in the shared statistics hashtable () and applies a user-provided reset function to determine which entries should have their statistics reset. For each entry that passes the reset criteria, it acquires an exclusive lock on the entry's header and calls  to reset the statistics data with the provided timestamp.

The function uses a sequential scan pattern through the distributed hash table, taking only shared locks during the scan since the hash entry structure itself is not modified, only the statistics content within each entry.

## Parameters / Member Variables
- : Function pointer that determines whether a particular statistics entry should be reset. Returns true if the entry should be reset.
- : Datum parameter passed to the  callback function, allowing the caller to provide context-specific matching criteria.
- : TimestampTz value used as the timestamp for the reset operation, typically the current time.

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes sequential scan of distributed hash table
  - : Gets next entry during sequential scan
  - : Gets address from distributed shared memory area
  - : Resets the actual statistics content
  - : Terminates the sequential scan
  - /: Lightweight locking primitives
- Called from (representative examples):
  - : Resets statistics counters
  - : Resets statistics of a specific kind

## Notes and Other Information
- The function skips dropped entries () during the scan
- Uses exclusive locking () when actually resetting statistics to ensure consistency
- The callback pattern allows for flexible filtering of which statistics entries to reset
- Part of PostgreSQL's shared memory statistics infrastructure

## Simplified Source

```c
void pgstat_reset_matching_entries(bool (*do_reset)(PgStatShared_HashEntry *, Datum),
                                   Datum match_data, TimestampTz ts) {
    dshash_seq_status scan_state;
    PgStatShared_HashEntry *entry;

    // Initialize sequential scan of shared hash table
    dshash_seq_init(&scan_state, pgStatLocal.shared_hash, false);

    // Scan through all entries in the hash table
    while ((entry = dshash_seq_next(&scan_state)) != NULL) {
        // Skip dropped entries
        if (entry->dropped)
            continue;

        // Check if this entry should be reset using callback
        if (!do_reset(entry, match_data))
            continue;

        // Get the statistics data for this entry
        PgStatShared_Common *stats_data = dsa_get_address(pgStatLocal.dsa, entry->body);

        // Reset the statistics with exclusive lock
        LWLockAcquire(&stats_data->lock, LW_EXCLUSIVE);
        shared_stat_reset_contents(entry->key.kind, stats_data, ts);
        LWLockRelease(&stats_data->lock);
    }

    // Clean up the scan
    dshash_seq_term(&scan_state);
}
```