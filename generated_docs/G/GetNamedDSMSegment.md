# GetNamedDSMSegment

## Location
[src/backend/storage/ipc/dsm_registry.c:131-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_registry.c#L131-L200)

## Overview
GetNamedDSMSegment creates or attaches to a named dynamic shared memory segment, providing a persistent, named interface for accessing shared memory across PostgreSQL processes.

## Definition
void *GetNamedDSMSegment(const char *name, size_t size, void (*init_callback)(void *ptr), bool *found)

## Detailed Description
GetNamedDSMSegment is the primary public interface for PostgreSQL's named DSM segment system. It provides a mechanism to create or attach to shared memory segments identified by name, allowing multiple processes to share data structures that persist beyond individual process lifetimes.

The function operates in two modes:
1. **Creation mode**: If a segment with the given name doesn't exist, it creates a new DSM segment, pins it in memory, stores its handle in the registry, and optionally calls an initialization callback.
2. **Attachment mode**: If a segment with the given name already exists, it verifies the size matches the request and attaches to the existing segment.

The function includes comprehensive error checking for invalid names, empty names, oversized names, zero sizes, and size mismatches. It ensures thread safety through the underlying dshash operations and proper memory context management.

## Parameters / Member Variables
- : The string identifier for the DSM segment (must be non-empty and within size limits)
- : The required size of the shared memory segment (must be non-zero)
- : Optional function pointer called to initialize newly created segments
- : Output parameter indicating whether the segment already existed (true) or was newly created (false)

## Dependencies
- Functions called/Symbols referenced:
  - [init_dsm_registry](../i/init_dsm_registry.md) (initializes the registry hash table)
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md) (searches or creates registry entries)
  - [dsm_create](../d/dsm_create.md) (creates new DSM segments)
  - [dsm_pin_segment](../d/dsm_pin_segment.md) (pins segment in memory)
  - [dsm_pin_mapping](../d/dsm_pin_mapping.md) (pins segment mapping)
  - [dsm_segment_handle](../d/dsm_segment_handle.md) (gets segment handle for storage)
  - [dsm_segment_address](../d/dsm_segment_address.md) (gets segment memory address)
  - [dsm_find_mapping](../d/dsm_find_mapping.md) (finds existing segment mapping)
  - [dsm_attach](../d/dsm_attach.md) (attaches to existing segment)
  - [dshash_release_lock](../d/dshash_release_lock.md) (releases hash table entry lock)
  - [DSMRegistryEntry](../D/DSMRegistryEntry.md) (structure for registry entries)
- Called from (representative examples):
  - [injection_init_shmem](../i/injection_init_shmem.md) (in injection points test module)
  - [tdr_attach_shmem](../t/tdr_attach_shmem.md) (in DSM registry test module)

## Notes and Other Information
- This function switches to TopMemoryContext to ensure any local DSM/DSA allocations persist
- Segments are automatically pinned to prevent premature cleanup
- Size validation ensures existing segments match requested sizes exactly
- The function is designed to be safe for concurrent access from multiple backends
- Critical component of PostgreSQL's infrastructure for shared data structures like background worker communication and extension-specific shared state
- Error handling includes specific messages for common failure modes like empty names, oversized names, and size mismatches

## Simplified Source
```c
void *GetNamedDSMSegment(const char *name, size_t size,
                         void (*init_callback)(void *ptr), bool *found) {
    DSMRegistryEntry *entry;
    MemoryContext oldcontext;
    void *ret;

    // Validate input parameters
    Assert(found);
    if (!name || *name == '\0')
        ereport(ERROR, (errmsg("DSM segment name cannot be empty")));
    if (strlen(name) >= offsetof(DSMRegistryEntry, handle))
        ereport(ERROR, (errmsg("DSM segment name too long")));
    if (size == 0)
        ereport(ERROR, (errmsg("DSM segment size must be nonzero")));

    // Switch to persistent memory context
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    // Initialize registry and find/create entry
    init_dsm_registry();
    entry = dshash_find_or_insert(dsm_registry_table, name, found);

    if (!(*found)) {
        // Create new segment
        dsm_segment *seg = dsm_create(size, 0);
        dsm_pin_segment(seg);
        dsm_pin_mapping(seg);
        entry->handle = dsm_segment_handle(seg);
        entry->size = size;
        ret = dsm_segment_address(seg);

        // Initialize if callback provided
        if (init_callback)
            (*init_callback)(ret);
    } else if (entry->size != size) {
        // Size mismatch error
        ereport(ERROR, (errmsg("requested DSM segment size does not match "
                               "size of existing segment")));
    } else {
        // Attach to existing segment
        dsm_segment *seg = dsm_find_mapping(entry->handle);
        if (seg == NULL) {
            seg = dsm_attach(entry->handle);
            if (seg == NULL)
                elog(ERROR, "could not map dynamic shared memory segment");
            dsm_pin_mapping(seg);
        }
        ret = dsm_segment_address(seg);
    }

    // Clean up and return
    dshash_release_lock(dsm_registry_table, entry);
    MemoryContextSwitchTo(oldcontext);
    return ret;
}
```