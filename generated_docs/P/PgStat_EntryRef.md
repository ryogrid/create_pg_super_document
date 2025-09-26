# PgStat_EntryRef

## Location
src/include/utils/pgstat_internal.h: 134 - 163

## Overview
PgStat_EntryRef is a backend-local reference to a shared statistics entry that provides caching and pending update management while ensuring the referenced shared entry remains valid.

## Definition


## Detailed Description
PgStat_EntryRef serves as a backend-local reference and cache for shared statistics entries. This structure is crucial for PostgreSQL's statistics architecture as it provides several key benefits: it maintains a reference to prevent shared entries from being freed prematurely, caches the resolved pointer to statistics data to avoid repeated DSA lookups, and manages pending statistics updates that haven't yet been flushed to shared memory.

The structure implements a two-level caching system where backends maintain local references (PgStat_EntryRef) that point to shared entries (PgStatShared_HashEntry), which in turn point to the actual statistics data. This design reduces contention on the shared hashtable by allowing most statistics operations to work with cached local references.

The pending mechanism allows backends to accumulate statistics updates locally without immediately writing to shared memory, improving performance by batching updates and reducing lock contention. The generation field helps detect when the shared entry has been reused, ensuring that stale local references can be detected and refreshed.

## Parameters / Member Variables
- : Pointer to the corresponding PgStatShared_HashEntry in the shared statistics hashtable
- : Cached local pointer to the actual statistics data, avoiding repeated dsa_get_address() calls
- : Local copy of the shared entry's generation counter to detect entry reuse
- : Pointer to pending statistics data that needs to be flushed; format is kind-specific
- : Double-linked list node for membership in the global pgStatPending list

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_HashEntry (shared hashtable entry type)
  - PgStatShared_Common (shared statistics data header)
  - dlist_node (double-linked list node)
- Called from (representative examples):
  - pgstat_get_entry_ref (reference acquisition)
  - pgstat_release_entry_ref (reference release)
  - pgstat_fetch_entry (statistics retrieval)
  - pgstat_prep_pending_entry (pending data preparation)
  - pgstat_flush_pending_entries (flush pending updates)
  - find_tabstat_entry (relation statistics lookup)
  - find_funcstat_entry (function statistics lookup)

## Notes and Other Information
- Acts as a reference-counted local cache for shared statistics entries, preventing premature deallocation
- The pending mechanism enables batched updates to improve performance and reduce shared memory contention
- Generation tracking helps detect and handle race conditions when shared entries are reused
- Each backend maintains its own hash table (pgStatEntryRefHash) of these local references
- The structure is part of a sophisticated three-tier architecture: local references → shared entries → actual statistics data
- Pending data format and flushing behavior is defined per statistics kind through PgStat_KindInfo callbacks
- Lifetime is managed through reference counting in the shared entry to ensure safe concurrent access