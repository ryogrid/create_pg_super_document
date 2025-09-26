# PgStat_EntryRef

## Location
[src/include/utils/pgstat_internal.h:134-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L134-L163)

## Overview
PgStat_EntryRef is a backend-local reference to a shared statistics entry that provides caching and pending update management while ensuring the referenced shared entry remains valid.

## Definition

```c
typedef struct PgStat_EntryRef
{
	/*
	 * Pointer to the PgStatShared_HashEntry entry in the shared stats
	 * hashtable.
	 */
	PgStatShared_HashEntry *shared_entry;

	/*
	 * Pointer to the stats data (i.e. PgStatShared_HashEntry->body), resolved
	 * as a local pointer, to avoid repeated dsa_get_address() calls.
	 */
	PgStatShared_Common *shared_stats;

	/*
	 * Copy of PgStatShared_HashEntry->generation, keeping locally track of
	 * the shared stats entry "generation" retrieved (number of times reused).
	 */
	uint32		generation;

	/*
	 * Pending statistics data that will need to be flushed to shared memory
	 * stats eventually. Each stats kind utilizing pending data defines what
	 * format its pending data has and needs to provide a
	 * PgStat_KindInfo->flush_pending_cb callback to merge pending into shared
	 * stats.
	 */
	void	   *pending;
	dlist_node	pending_node;	/* membership in pgStatPending list */
} PgStat_EntryRef;
```
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
  - [PgStatShared_HashEntry](PgStatShared_HashEntry.md) (shared hashtable entry type)
  - [PgStatShared_Common](PgStatShared_Common.md) (shared statistics data header)
  - [dlist_node](../d/dlist_node.md) (double-linked list node)
- Called from (representative examples):
  - [pgstat_get_entry_ref](../p/pgstat_get_entry_ref.md) (reference acquisition)
  - [pgstat_release_entry_ref](../p/pgstat_release_entry_ref.md) (reference release)
  - [pgstat_fetch_entry](../p/pgstat_fetch_entry.md) (statistics retrieval)
  - [pgstat_prep_pending_entry](../p/pgstat_prep_pending_entry.md) (pending data preparation)
  - [pgstat_flush_pending_entries](../p/pgstat_flush_pending_entries.md) (flush pending updates)
  - [find_tabstat_entry](../f/find_tabstat_entry.md) (relation statistics lookup)
  - [find_funcstat_entry](../f/find_funcstat_entry.md) (function statistics lookup)

## Notes and Other Information
- Acts as a reference-counted local cache for shared statistics entries, preventing premature deallocation
- The pending mechanism enables batched updates to improve performance and reduce shared memory contention
- Generation tracking helps detect and handle race conditions when shared entries are reused
- Each backend maintains its own hash table (pgStatEntryRefHash) of these local references
- The structure is part of a sophisticated three-tier architecture: local references → shared entries → actual statistics data
- Pending data format and flushing behavior is defined per statistics kind through PgStat_KindInfo callbacks
- Lifetime is managed through reference counting in the shared entry to ensure safe concurrent access