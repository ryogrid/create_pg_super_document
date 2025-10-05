# do_start_worker

## Location
[src/backend/postmaster/autovacuum.c:1073-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1073-L1284)

## Overview
A bare-bones procedure for starting an autovacuum worker from the launcher that determines what database to work on, sets up shared memory structures, and signals the postmaster to start the worker.

## Definition

```c
struct a metric that measures that and not cause
	 * starvation for less busy databases.
	 */
	avdb = NULL;
```
## Detailed Description
The  function implements the core logic for launching autovacuum workers from the autovacuum launcher. It performs intelligent database selection by prioritizing databases that are at risk of transaction ID wraparound or MultiXact ID wraparound, followed by databases that haven't been auto-vacuumed recently.

The function first checks if any worker slots are available in the shared memory worker pool. If no workers are free, it returns immediately. Otherwise, it creates a temporary memory context and retrieves a list of all databases in the cluster.

The database selection algorithm prioritizes databases based on urgency:
1. Databases at risk of XID wraparound (datfrozenxid older than autovacuum_freeze_max_age)
2. Databases at risk of MultiXact wraparound (datminmxid older than MultiXactMemberFreezeThreshold)
3. Databases with the oldest last_autovac_time from pgstat

The function also implements throttling by skipping databases that were recently processed (within autovacuum_naptime seconds) according to the internal DatabaseList tracking.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - / (AutovacuumLock coordination)
  -  (check for available workers)
  - / (memory management)
  -  (retrieve database information)
  - / (get current transaction IDs)
  - / (wraparound risk detection)
  -  (database statistics)
  -  (time-based throttling)
  -  (PMSIGNAL_START_AUTOVAC_WORKER)
  -  (database list maintenance)

- Called from (representative examples):
  -  (src/backend/postmaster/autovacuum.c:1290)

## Notes and Other Information
- Returns the OID of the database that will be processed, or InvalidOid if no worker was started
- Fails gracefully when autovacuum_workers are already at capacity
- Uses sophisticated prioritization to prevent transaction ID wraparound disasters
- Implements temporal throttling to avoid repeatedly selecting the same database
- Creates temporary memory context to prevent memory leaks during database list processing
- Critical for maintaining database health in PostgreSQL clusters with multiple databases

## Simplified Source

```c
static Oid do_start_worker(void)
{
    // Quick exit if no workers available
    LWLockAcquire(AutovacuumLock, LW_SHARED);
    if (dlist_is_empty(&AutoVacuumShmem->av_freeWorkers)) {
        LWLockRelease(AutovacuumLock);
        return InvalidOid;
    }
    LWLockRelease(AutovacuumLock);

    // Create temporary context and get database list
    MemoryContext tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                                "Autovacuum start worker (tmp)",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(tmpcxt);
    List *dblist = get_database_list();

    // Calculate wraparound thresholds
    TransactionId xidForceLimit = ReadNextTransactionId() - autovacuum_freeze_max_age;
    MultiXactId multiForceLimit = ReadNextMultiXactId() - MultiXactMemberFreezeThreshold();

    // Select database using priority algorithm
    avw_dbase *selected_db = NULL;
    bool for_xid_wrap = false;
    bool for_multi_wrap = false;
    TimestampTz current_time = GetCurrentTimestamp();

    foreach(cell, dblist) {
        avw_dbase *db = lfirst(cell);

        // Priority 1: XID wraparound danger
        if (TransactionIdPrecedes(db->adw_frozenxid, xidForceLimit)) {
            if (selected_db == NULL ||
                TransactionIdPrecedes(db->adw_frozenxid, selected_db->adw_frozenxid)) {
                selected_db = db;
                for_xid_wrap = true;
            }
            continue;
        }
        if (for_xid_wrap) continue;

        // Priority 2: MultiXact wraparound danger
        if (MultiXactIdPrecedes(db->adw_minmulti, multiForceLimit)) {
            if (selected_db == NULL ||
                MultiXactIdPrecedes(db->adw_minmulti, selected_db->adw_minmulti)) {
                selected_db = db;
                for_multi_wrap = true;
            }
            continue;
        }
        if (for_multi_wrap) continue;

        // Priority 3: Oldest last autovac time (with throttling check)
        db->adw_entry = pgstat_fetch_stat_dbentry(db->adw_datid);
        if (!db->adw_entry) continue;

        // Skip if recently processed
        if (database_recently_processed(db->adw_datid, current_time))
            continue;

        if (selected_db == NULL ||
            db->adw_entry->last_autovac_time < selected_db->adw_entry->last_autovac_time) {
            selected_db = db;
        }
    }

    // Start worker if database selected
    Oid result = InvalidOid;
    if (selected_db != NULL) {
        LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);

        WorkerInfo worker = dlist_container(WorkerInfoData, wi_links,
                                          dlist_pop_head_node(&AutoVacuumShmem->av_freeWorkers));
        worker->wi_dboid = selected_db->adw_datid;
        worker->wi_proc = NULL;
        worker->wi_launchtime = GetCurrentTimestamp();

        AutoVacuumShmem->av_startingWorker = worker;
        LWLockRelease(AutovacuumLock);

        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);
        result = selected_db->adw_datid;
    }

    // Clean up and return
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(tmpcxt);
    return result;
}
```