# pgstat_fetch_entry

## Location
src/backend/utils/activity/pgstat.c: 811 - 906

## Overview
This function fetches statistics data for a specific database object identified by kind, database OID, and object OID, handling different consistency levels and caching strategies.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's statistics fetching infrastructure that retrieves statistics data for individual database objects. It supports different consistency models ranging from no caching to full snapshot consistency, and handles memory management appropriately for each mode.

The function operates by first constructing a hash key from the provided parameters, then determining the appropriate fetching strategy based on the current  setting. For snapshot consistency, it may build a complete snapshot first. For cache consistency, it maintains cached entries to avoid repeated expensive lookups.

The function handles several edge cases: it returns NULL for dropped entries, creates empty cache entries when appropriate, and manages memory allocation differently depending on the consistency mode to optimize performance and memory usage.

## Parameters / Member Variables
- : A  enum value specifying the type of statistics to fetch
- : The OID of the database containing the object
- : The OID of the specific object whose statistics are being requested

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_kind_info
  - pgstat_prep_snapshot
  - pgstat_build_snapshot
  - pgstat_snapshot_lookup
  - pgstat_get_entry_ref
  - pgstat_snapshot_insert
  - pgstat_lock_entry_shared
  - pgstat_get_entry_data
  - pgstat_unlock_entry
  - MemoryContextAlloc
  - PgStat_HashKey (struct type)
  - PgStat_EntryRef (struct type)
  - PgStat_SnapshotEntry (struct type)
- Called from (representative examples):
  - pgstat_fetch_stat_dbentry
  - pgstat_fetch_stat_funcentry
  - pgstat_fetch_stat_tabentry_ext
  - pgstat_fetch_replslot
  - pgstat_fetch_stat_subscription

## Notes and Other Information
- This function should only be called from backend processes, not the postmaster
- Only supports statistics kinds with variable amounts (not fixed_amount kinds)
- Memory allocation strategy varies by consistency mode: caller's context for NONE, snapshot context for others
- Handles three consistency levels: NONE (no caching), CACHE (individual entry caching), and SNAPSHOT (full snapshot)
- Thread-safe through proper locking of shared statistics entries
- Returns NULL when no statistics exist for the requested object or if the object has been dropped
- The function clears padding in the hash key structure to ensure consistent hash values