# SharedRecordTypmodRegistry

## Location
[src/backend/utils/cache/typcache.c:166-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L166-L181)

## Overview
SharedRecordTypmodRegistry is a structure that manages shared record type definitions across multiple backend processes involved in parallel queries.

## Definition
```c
struct SharedRecordTypmodRegistry
{
    /* A hash table for finding a matching TupleDesc. */
    dshash_table_handle record_table_handle;
    /* A hash table for finding a TupleDesc by typmod. */
    dshash_table_handle typmod_table_handle;
    /* A source of new record typmod numbers. */
    pg_atomic_uint32 next_typmod;
};
```

## Detailed Description
SharedRecordTypmodRegistry provides a shared memory-based registry for non-anonymous record types that need to be exchanged between backend processes participating in parallel queries. Unlike the per-backend RecordCacheEntry system, this registry allows multiple backends to share consistent record type definitions and typmod assignments across process boundaries.

The structure maintains two hash tables: one for finding matching TupleDescs based on their content, and another for direct lookup by typmod. This dual-indexing approach enables efficient sharing of record type information while maintaining the same performance characteristics as the single-backend cache. The atomic counter ensures thread-safe generation of unique typmod values across all participating backends.

## Parameters / Member Variables
- `record_table_handle`: Handle to a shared hash table used for finding existing TupleDesc entries that match a given record type definition
- `typmod_table_handle`: Handle to a shared hash table that provides direct lookup of TupleDesc entries using their assigned typmod values
- `next_typmod`: Atomic counter that generates unique typmod numbers across all backends in a thread-safe manner

## Dependencies
- Functions called/Symbols referenced:
  - dshash_table_handle (PostgreSQL dynamic shared hash table handle)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (PostgreSQL atomic 32-bit unsigned integer)
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor structure)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md)
  - [AttachSession](../A/AttachSession.md)
  - [SharedRecordTypmodRegistryEstimate](SharedRecordTypmodRegistryEstimate.md)
  - [SharedRecordTypmodRegistryInit](SharedRecordTypmodRegistryInit.md)
  - [SharedRecordTypmodRegistryAttach](SharedRecordTypmodRegistryAttach.md)

## Notes and Other Information
- Designed specifically for parallel query execution where multiple backends need consistent record type definitions
- Uses PostgreSQL's dynamic shared hash table infrastructure for efficient shared memory management
- The atomic counter ensures that typmod assignment is thread-safe across multiple concurrent backends
- Part of the broader session and parallel query infrastructure in PostgreSQL
- Located in src/backend/utils/cache/typcache.c as an extension of the type cache system
- Works in conjunction with the Session system for managing shared query state
- Provides the same dual-indexing benefits as the single-backend system but in shared memory