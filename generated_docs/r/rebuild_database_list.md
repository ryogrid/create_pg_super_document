# rebuild_database_list

## Location
src/backend/postmaster/autovacuum.c: 876 - 1054

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
  - hash_create
  - pgstat_fetch_stat_dbentry
  - hash_search
  - get_database_list
  - dlist_init
  - qsort (with db_comparator)
  - GetCurrentTimestamp
  - TimestampTzPlusMilliseconds
  - dlist_push_head
  - MemoryContextDelete
- Data structures used:
  - avl_dbase
  - HASHTBL
  - dlist_iter
  - PgStat_StatDBEntry
- Global variables accessed:
  - DatabaseList
  - DatabaseListCxt
  - AutovacMemCxt
  - autovacuum_naptime
  - MIN_AUTOVAC_SLEEPTIME
- Called from:
  - HandleAutoVacLauncherInterrupts (config reload - line 756)
  - launcher_determine_sleep (time rebalancing - line 840)
  - do_start_worker (worker launch handling - line 1264)
  - launch_worker (worker management - line 1327)

## Notes and Other Information
- Uses a three-phase database insertion algorithm: new database first, existing databases second, catalog databases last
- Implements intelligent time distribution to prevent scheduling conflicts and resource contention
- Only includes databases that have PostgreSQL statistics entries (filters out dropped databases)
- Uses a temporary hash table for efficient duplicate detection during list construction
- Automatically adjusts scheduling intervals if configured naptime would result in too-short sleep periods
- Memory management creates a new context for the database list and cleans up the old context
- The scheduling algorithm ensures databases are distributed evenly across the naptime interval
- Critical for autovacuum performance and preventing database maintenance backlogs