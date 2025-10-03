# SharedRecordTypmodRegistryInit

## Location
[src/backend/utils/cache/typcache.c:2108-2206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2108-L2206)

## Overview
Initializes a SharedRecordTypmodRegistry in shared memory for parallel query execution, creating hash tables for tuple descriptor management and populating them with the current backend's record types.

## Definition

```c
void
SharedRecordTypmodRegistryInit(SharedRecordTypmodRegistry *registry,
							   dsm_segment *segment,
							   dsa_area *area)
```
## Detailed Description
This function initializes a shared record typmod registry that allows multiple parallel backends to coordinate and share non-anonymous record type definitions. It sets up two hash tables: one indexed by tuple descriptors themselves and another indexed by typmod numbers. The function migrates all existing record types from the current backend's private registry into the shared registry.

The initialization process involves creating dshash tables in shared memory, copying tuple descriptors from the local cache to shared memory using share_tupledesc(), and establishing global state that redirects subsequent record type operations to use the shared registry. The function also sets up cleanup mechanisms via DSM detach hooks.

This function is called by the leader process in parallel query execution and automatically attaches the leader to the shared registry. Worker processes must attach separately using SharedRecordTypmodRegistryAttach().

## Parameters / Member Variables
- : Pointer to the SharedRecordTypmodRegistry structure to initialize in shared memory
- : DSM segment containing the shared memory area 
- : DSA area for allocating additional shared memory as needed for typmod registration

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [dshash_create](../d/dshash_create.md)
  - [dshash_get_hash_table_handle](../d/dshash_get_hash_table_handle.md)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)
  - [share_tupledesc](../s/share_tupledesc.md)
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md)
  - [dshash_release_lock](../d/dshash_release_lock.md)
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [shared_record_typmod_registry_detach](../s/shared_record_typmod_registry_detach.md)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md)

## Notes and Other Information
- Must be called by the leader process only (asserts !IsParallelWorker())
- Cannot be called if already attached to a shared registry
- Copies all existing record types from RecordCacheArray to shared memory
- Sets up global CurrentSession state to redirect future record type operations to shared registry
- Installs DSM detach hook for cleanup on failure during GetSessionDsmHandle()
- Uses TopMemoryContext for hash table creation to ensure proper memory management
- The shared registry remains active until process exit for the leader process

## Simplified Source

```c
void SharedRecordTypmodRegistryInit(SharedRecordTypmodRegistry *registry,
                                  dsm_segment *segment, dsa_area *area) {
    MemoryContext old_context;
    dshash_table *record_table;
    dshash_table *typmod_table;
    int32 typmod;

    // Must be called by leader process only
    Assert(!IsParallelWorker());
    Assert(CurrentSession->shared_typmod_registry == NULL);

    old_context = MemoryContextSwitchTo(TopMemoryContext);

    // Create hash tables for record type management
    record_table = dshash_create(area, &srtr_record_table_params, area);
    typmod_table = dshash_create(area, &srtr_typmod_table_params, NULL);

    MemoryContextSwitchTo(old_context);

    // Initialize registry with hash table handles and typmod counter
    registry->record_table_handle = dshash_get_hash_table_handle(record_table);
    registry->typmod_table_handle = dshash_get_hash_table_handle(typmod_table);
    pg_atomic_init_u32(&registry->next_typmod, NextRecordTypmod);

    // Copy all existing record types from private to shared registry
    for (typmod = 0; typmod < NextRecordTypmod; ++typmod) {
        TupleDesc tupdesc = RecordCacheArray[typmod].tupdesc;
        if (tupdesc == NULL)
            continue;

        // Share the tuple descriptor and insert into both hash tables
        dsa_pointer shared_dp = share_tupledesc(area, tupdesc, typmod);

        // Insert into typmod table
        SharedTypmodTableEntry *typmod_entry =
            dshash_find_or_insert(typmod_table, &tupdesc->tdtypmod, &found);
        if (found)
            elog(ERROR, "cannot create duplicate shared record typmod");
        typmod_entry->typmod = tupdesc->tdtypmod;
        typmod_entry->shared_tupdesc = shared_dp;
        dshash_release_lock(typmod_table, typmod_entry);

        // Insert into record table
        SharedRecordTableKey record_key;
        record_key.shared = false;
        record_key.u.local_tupdesc = tupdesc;
        SharedRecordTableEntry *record_entry =
            dshash_find_or_insert(record_table, &record_key, &found);
        if (!found) {
            record_entry->key.shared = true;
            record_entry->key.u.shared_tupdesc = shared_dp;
        }
        dshash_release_lock(record_table, record_entry);
    }

    // Activate shared registry for this session
    CurrentSession->shared_record_table = record_table;
    CurrentSession->shared_typmod_table = typmod_table;
    CurrentSession->shared_typmod_registry = registry;

    // Register cleanup hook
    on_dsm_detach(segment, shared_record_typmod_registry_detach, (Datum) 0);
}
```