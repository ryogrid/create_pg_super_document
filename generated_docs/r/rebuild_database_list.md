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