# pgstat_get_entry_ref

## Location
[src/backend/utils/activity/pgstat_shmem.c:418-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L418-L549)

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
  - [dshash_find](../d/dshash_find.md)
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md)
  - [pgstat_init_entry](pgstat_init_entry.md)
  - [pgstat_acquire_entry_ref](pgstat_acquire_entry_ref.md)
  - [pgstat_release_entry_ref](pgstat_release_entry_ref.md)
  - [pgstat_reinit_entry](pgstat_reinit_entry.md)
  - [dshash_release_lock](../d/dshash_release_lock.md)
  - [dsa_get_address](../d/dsa_get_address.md)
- Called from (representative examples):
  - [pgstat_fetch_entry](pgstat_fetch_entry.md)
  - [pgstat_have_entry](pgstat_have_entry.md)
  - [pgstat_prep_pending_entry](pgstat_prep_pending_entry.md)
  - [pgstat_fetch_pending_entry](pgstat_fetch_pending_entry.md)

## Notes and Other Information
The function implements a garbage collection check for dropped entries that couldn't be deleted due to outstanding references. The local cache optimization significantly reduces contention on the shared hash table. Entry reinitialization handles legitimate cases where old stats entries are reused before being fully dropped.

## Simplified Source

```c
// Simplified version of pgstat_get_entry_ref
PgStat_EntryRef *
pgstat_get_entry_ref(PgStat_Kind kind, Oid dboid, Oid objoid, bool create,
                     bool *created_entry)
{
    PgStat_HashKey key;
    PgStatShared_HashEntry *shhashent;
    PgStat_EntryRef *entry_ref;

    // Build lookup key
    memset(&key, 0, sizeof(key));
    key.kind = kind;
    key.dboid = dboid;
    key.objoid = objoid;

    // Initialize memory context and shared references
    pgstat_setup_memcxt();
    pgstat_setup_shared_refs();

    if (created_entry != NULL)
        *created_entry = false;

    // Garbage collect dropped entries if needed
    if (pgstat_need_entry_refs_gc())
        pgstat_gc_entry_refs();

    // Check local cache first to avoid locks
    if (pgstat_get_entry_ref_cached(key, &entry_ref))
        return entry_ref;

    // Look up entry in shared hash table
    shhashent = dshash_find(pgStatLocal.shared_hash, &key, false);

    // Create entry if requested and not found
    if (create && !shhashent) {
        bool found;
        shhashent = dshash_find_or_insert(pgStatLocal.shared_hash, &key, &found);

        if (!found) {
            // Initialize new entry
            pgstat_init_entry(kind, shhashent);
            pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);

            if (created_entry != NULL)
                *created_entry = true;

            return entry_ref;
        }
    }

    // Handle case where entry not found and not creating
    if (!shhashent) {
        pgstat_release_entry_ref(key, entry_ref, false);
        return NULL;
    }

    // Handle existing entry
    if (shhashent->dropped && create) {
        // Reinitialize dropped entry for reuse
        pgstat_reinit_entry(kind, shhashent);
        pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);

        if (created_entry != NULL)
            *created_entry = true;

        return entry_ref;
    }
    else if (shhashent->dropped) {
        // Entry is dropped and we're not creating
        dshash_release_lock(pgStatLocal.shared_hash, shhashent);
        pgstat_release_entry_ref(key, entry_ref, false);
        return NULL;
    }
    else {
        // Use existing active entry
        pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);
        return entry_ref;
    }
}
```

Key simplifications made:
- Removed detailed comments and assertions for clarity
- Consolidated variable declarations at the top
- Simplified the control flow logic for better readability
- Removed platform-specific optimizations and error handling details
- Added high-level comments explaining each major step
- Abstracted complex shared memory operations into descriptive function calls
- Maintained the essential algorithm: cache check → hash lookup → create/reuse logic