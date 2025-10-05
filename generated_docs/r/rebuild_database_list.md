# rebuild_database_list

## Location
[src/backend/postmaster/autovacuum.c:876-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L876-L1054)

## Overview
Constructs an updated list of databases that need autovacuum maintenance, sorted by scheduling priority and distributed evenly across the autovacuum naptime interval.

## Definition
static void rebuild_database_list(Oid newdb)

## Detailed Description
This function rebuilds the global DatabaseList used by the autovacuum launcher to schedule vacuum operations across databases. It implements a sophisticated scheduling algorithm that ensures even distribution of autovacuum work over time while respecting database priorities. The function processes databases in three phases: first adding any new database, then preserving the order of existing databases, and finally adding any remaining databases from the system catalog. The resulting list is sorted and scheduled to distribute maintenance work evenly across the autovacuum_naptime interval, preventing resource contention and ensuring fair scheduling.

## Parameters / Member Variables
- : OID of a database that triggered this rebuild, or InvalidOid if this is a general rebuild

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [hash_create](../h/hash_create.md)
  - [pgstat_fetch_stat_dbentry](../p/pgstat_fetch_stat_dbentry.md)
  - [hash_search](../h/hash_search.md)
  - [get_database_list](../g/get_database_list.md)
  - [dlist_init](../d/dlist_init.md)
  - qsort (with db_comparator)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - TimestampTzPlusMilliseconds
  - [dlist_push_head](../d/dlist_push_head.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Data structures used:
  - [avl_dbase](../a/avl_dbase.md)
  - HASHTBL
  - [dlist_iter](../d/dlist_iter.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md)
- Global variables accessed:
  - DatabaseList
  - DatabaseListCxt
  - AutovacMemCxt
  - autovacuum_naptime
  - MIN_AUTOVAC_SLEEPTIME
- Called from:
  - [HandleAutoVacLauncherInterrupts](../H/HandleAutoVacLauncherInterrupts.md) (config reload - line 756)
  - [launcher_determine_sleep](../l/launcher_determine_sleep.md) (time rebalancing - line 840)
  - [do_start_worker](../d/do_start_worker.md) (worker launch handling - line 1264)
  - [launch_worker](../l/launch_worker.md) (worker management - line 1327)

## Notes and Other Information
- Uses a three-phase database insertion algorithm: new database first, existing databases second, catalog databases last
- Implements intelligent time distribution to prevent scheduling conflicts and resource contention
- Only includes databases that have PostgreSQL statistics entries (filters out dropped databases)
- Uses a temporary hash table for efficient duplicate detection during list construction
- Automatically adjusts scheduling intervals if configured naptime would result in too-short sleep periods
- Memory management creates a new context for the database list and cleans up the old context
- The scheduling algorithm ensures databases are distributed evenly across the naptime interval
- Critical for autovacuum performance and preventing database maintenance backlogs

## Simplified Source

```c
static void rebuild_database_list(Oid newdb)
{
    // Create memory contexts for the new database list
    MemoryContext newcxt = AllocSetContextCreate(AutovacMemCxt, "Autovacuum database list", ALLOCSET_DEFAULT_SIZES);
    MemoryContext tmpcxt = AllocSetContextCreate(newcxt, "Autovacuum database list (tmp)", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(tmpcxt);

    // Create hash table to track databases by OID and assign scores
    HTAB *dbhash = hash_create("autovacuum db hash", 20, &hctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    int score = 0;

    // Phase 1: Add the new database with lowest score (highest priority)
    if (OidIsValid(newdb)) {
        PgStat_StatDBEntry *entry = pgstat_fetch_stat_dbentry(newdb);
        if (entry != NULL) {
            avl_dbase *db = hash_search(dbhash, &newdb, HASH_ENTER, NULL);
            db->adl_score = score++;
        }
    }

    // Phase 2: Add existing databases from current list, preserving order
    dlist_foreach(iter, &DatabaseList) {
        avl_dbase *existing_db = dlist_container(avl_dbase, adl_node, iter.cur);
        PgStat_StatDBEntry *entry = pgstat_fetch_stat_dbentry(existing_db->adl_datid);

        if (entry != NULL) {
            bool found;
            avl_dbase *db = hash_search(dbhash, &(existing_db->adl_datid), HASH_ENTER, &found);
            if (!found)
                db->adl_score = score++;
        }
    }

    // Phase 3: Add remaining databases from system catalog
    List *dblist = get_database_list();
    foreach(cell, dblist) {
        avw_dbase *catalog_db = lfirst(cell);
        PgStat_StatDBEntry *entry = pgstat_fetch_stat_dbentry(catalog_db->adw_datid);

        if (entry != NULL) {
            bool found;
            avl_dbase *db = hash_search(dbhash, &(catalog_db->adw_datid), HASH_ENTER, &found);
            if (!found)
                db->adl_score = score++;
        }
    }

    // Build sorted array and schedule databases evenly across naptime
    MemoryContextSwitchTo(newcxt);
    dlist_init(&DatabaseList);

    if (score > 0) {
        // Copy hash entries to array and sort by score
        avl_dbase *dbary = palloc(score * sizeof(avl_dbase));
        hash_seq_init(&seq, dbhash);
        for (int i = 0; i < score; i++) {
            avl_dbase *db = hash_seq_search(&seq);
            memcpy(&(dbary[i]), db, sizeof(avl_dbase));
        }
        qsort(dbary, score, sizeof(avl_dbase), db_comparator);

        // Calculate time intervals and assign schedule times
        int millis_increment = 1000.0 * autovacuum_naptime / score;
        if (millis_increment <= MIN_AUTOVAC_SLEEPTIME)
            millis_increment = MIN_AUTOVAC_SLEEPTIME * 1.1;

        TimestampTz current_time = GetCurrentTimestamp();
        for (int i = 0; i < score; i++) {
            current_time = TimestampTzPlusMilliseconds(current_time, millis_increment);
            dbary[i].adl_next_worker = current_time;
            dlist_push_head(&DatabaseList, &dbary[i].adl_node);
        }
    }

    // Clean up old context and switch to new one
    if (DatabaseListCxt != NULL)
        MemoryContextDelete(DatabaseListCxt);
    MemoryContextDelete(tmpcxt);
    DatabaseListCxt = newcxt;
}
```