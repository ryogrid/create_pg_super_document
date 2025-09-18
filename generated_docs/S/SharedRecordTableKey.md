# SharedRecordTableKey

## Location
src/backend/utils/cache/typcache.c: 182 - 190

## Overview
SharedRecordTableKey is a versatile structure that serves as a hash table key for both local and shared tuple descriptors in the shared record type registry.

## Definition
```c
typedef struct SharedRecordTableKey
{
    union
    {
        TupleDesc   local_tupdesc;
        dsa_pointer shared_tupdesc;
    }           u;
    bool        shared;
} SharedRecordTableKey;
```

## Detailed Description
SharedRecordTableKey provides a unified interface for hash table operations that need to work with both backend-local TupleDesc structures and shared TupleDesc structures stored in dynamic shared memory. This design allows the shared record type registry to efficiently search for equivalent tuple descriptors regardless of whether they are stored locally or in shared memory.

The structure uses a union to store either a local TupleDesc pointer or a dsa_pointer to a shared TupleDesc, with a boolean flag indicating which type is currently stored. This approach enables the hash and comparison functions to handle both types transparently, allowing searches for matching shared TupleDescs using backend-local TupleDescs as keys.

## Parameters / Member Variables
- `u.local_tupdesc`: Pointer to a backend-local TupleDesc structure (when shared is false)
- `u.shared_tupdesc`: dsa_pointer to a TupleDesc structure stored in dynamic shared memory (when shared is true)
- `shared`: Boolean flag indicating whether the union contains a local TupleDesc (false) or a shared TupleDesc pointer (true)

## Dependencies
- Functions called/Symbols referenced:
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor structure)
  - dsa_pointer (PostgreSQL dynamic shared memory pointer)
- Called from (representative examples):
  - [SharedRecordTableEntry](SharedRecordTableEntry.md)
  - [shared_record_table_compare](../s/shared_record_table_compare.md)
  - [shared_record_table_hash](../s/shared_record_table_hash.md)
  - [SharedRecordTypmodRegistryInit](SharedRecordTypmodRegistryInit.md)
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md)

## Notes and Other Information
- Essential component of the shared record type system for parallel query processing
- Enables efficient lookups in shared hash tables using either local or shared tuple descriptors as keys
- The union design minimizes memory usage while supporting both storage types
- Used specifically in hash table operations where both local and shared descriptors need to be compared
- Part of the PostgreSQL dynamic shared memory and parallel query infrastructure
- Located in src/backend/utils/cache/typcache.c as part of the shared record type caching system
- Works with specialized hash and comparison functions that understand both descriptor types