# PgStatShared_HashEntry

## Location
src/include/utils/pgstat_internal.h: 64 - 115

## Overview
PgStatShared_HashEntry represents an entry in PostgreSQL's shared statistics hashtable, containing metadata and a pointer to the actual variable-sized statistics data rather than the data itself.

## Definition

```c
typedef struct PgStatShared_HashEntry
{
	PgStat_HashKey key;			/* hash key */

	/*
	 * If dropped is set, backends need to release their references so that
	 * the memory for the entry can be freed. No new references may be made
	 * once marked as dropped.
	 */
	bool		dropped;

	/*
	 * Refcount managing lifetime of the entry itself (as opposed to the
	 * dshash entry pointing to it). The stats lifetime has to be separate
	 * from the hash table entry lifetime because we allow backends to point
	 * to a stats entry without holding a hash table lock (and some other
	 * reasons).
	 *
	 * As long as the entry is not dropped, 1 is added to the refcount
	 * representing that the entry should not be dropped. In addition each
	 * backend that has a reference to the entry needs to increment the
	 * refcount as long as it does.
	 *
	 * May only be incremented / decremented while holding at least a shared
	 * lock on the dshash partition containing the entry. It needs to be an
	 * atomic variable because multiple backends can increment the refcount
	 * with just a shared lock.
	 *
	 * When the refcount reaches 0 the entry needs to be freed.
	 */
	pg_atomic_uint32 refcount;

	/*
	 * Counter tracking the number of times the entry has been reused.
	 *
	 * Set to 0 when the entry is created, and incremented by one each time
	 * the shared entry is reinitialized with pgstat_reinit_entry().
	 *
	 * May only be incremented / decremented while holding at least a shared
	 * lock on the dshash partition containing the entry. Like refcount, it
	 * needs to be an atomic variable because multiple backends can increment
	 * the generation with just a shared lock.
	 */
	pg_atomic_uint32 generation;

	/*
	 * Pointer to shared stats. The stats entry always starts with
	 * PgStatShared_Common, embedded in a larger struct containing the
	 * PgStat_Kind specific stats fields.
	 */
	dsa_pointer body;
} PgStatShared_HashEntry;
```
## Detailed Description
PgStatShared_HashEntry is the core structure for the shared statistics hashtable in PostgreSQL. Unlike containing the statistics data directly, it uses a pointer-based design where the actual variable-sized statistics are stored separately and referenced via the body field. This design allows for efficient memory management of statistics entries of different sizes.

The structure implements a sophisticated reference counting mechanism to manage the lifecycle of statistics entries. The refcount field ensures that entries are only freed when all backends have released their references, preventing use-after-free errors in a multi-backend environment. The dropped flag provides a safe deletion mechanism where entries can be marked for deletion while still allowing existing references to complete their operations.

The generation counter tracks how many times an entry has been reused, which helps detect stale references and supports entry recycling. All atomic operations on refcount and generation require at least a shared lock on the dshash partition to ensure consistency across multiple backends.

## Parameters / Member Variables
- : PgStat_HashKey structure that uniquely identifies this statistics entry (kind, dboid, objoid)
- : Boolean flag indicating the entry is marked for deletion and no new references should be made
- : Atomic reference counter managing the entry's lifetime; entry is freed when this reaches zero
- : Atomic counter incremented each time the entry is reinitialized, used to detect stale references
- : Dynamic shared area (DSA) pointer to the actual statistics data, which starts with PgStatShared_Common

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_HashKey (key structure)
  - pg_atomic_uint32 (atomic integer type)
  - dsa_pointer (dynamic shared area pointer)
- Called from (representative examples):
  - pgstat_init_entry (entry initialization)
  - pgstat_reinit_entry (entry reinitialization) 
  - pgstat_get_entry_ref (reference acquisition)
  - pgstat_release_entry_ref (reference release)
  - pgstat_drop_entry (entry deletion)
  - pgstat_build_snapshot (snapshot creation)

## Notes and Other Information
- The separation of entry metadata from actual statistics data allows for variable-sized statistics while maintaining efficient hashtable operations
- Reference counting is essential for safe multi-backend access without requiring exclusive locks for most operations
- The generation counter helps detect and handle race conditions when entries are reused
- All atomic operations require proper locking of the dshash partition for consistency
- The body pointer always points to statistics data beginning with PgStatShared_Common, followed by kind-specific fields
- Entry lifecycle: created → referenced → potentially dropped → dereferenced → freed when refcount reaches zero