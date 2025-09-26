# PgStatShared_HashEntry

## Location
src/include/utils/pgstat_internal.h: 64 - 115

## Overview
PgStatShared_HashEntry represents an entry in PostgreSQL's shared statistics hashtable, containing metadata and a pointer to the actual variable-sized statistics data rather than the data itself.

## Definition


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