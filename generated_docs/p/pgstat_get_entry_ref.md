# pgstat_get_entry_ref

## Location
src/backend/utils/activity/pgstat_shmem.c: 418 - 549

## Overview
Gets a shared statistics reference, creating the shared statistics object if requested and it does not exist.

## Definition
PgStat_EntryRef *pgstat_get_entry_ref(PgStat_Kind kind, Oid dboid, Oid objoid, bool create, bool *created_entry)

## Detailed Description
This function manages access to PostgreSQL's shared statistics entries through a reference counting mechanism. It first checks a local cache to avoid expensive shared memory operations when possible. If not cached, it performs a lookup in the shared hash table using dshash_find(). When create is true and the entry doesn't exist, it uses dshash_find_or_insert() to atomically create the entry. The function handles entry reinitialization for dropped entries that are being reused (common with replication slots and OID wraparound scenarios). It implements proper locking and reference counting to ensure thread-safe access to shared statistics data.

## Parameters / Member Variables
- : The type of statistics object (database, relation, function, etc.)
- : Database OID for the statistics entry
- : Object OID for the statistics entry  
- : Whether to create the entry if it doesn't exist
- : Output parameter set to true if entry was newly created, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - dshash_find
  - dshash_find_or_insert
  - pgstat_init_entry
  - pgstat_acquire_entry_ref
  - pgstat_release_entry_ref
  - pgstat_reinit_entry
  - dshash_release_lock
  - dsa_get_address
- Called from (representative examples):
  - pgstat_fetch_entry
  - pgstat_have_entry
  - pgstat_prep_pending_entry
  - pgstat_fetch_pending_entry

## Notes and Other Information
The function implements a garbage collection check for dropped entries that couldn't be deleted due to outstanding references. The local cache optimization significantly reduces contention on the shared hash table. Entry reinitialization handles legitimate cases where old stats entries are reused before being fully dropped.