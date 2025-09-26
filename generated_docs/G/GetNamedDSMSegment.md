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
  - init_dsm_registry (initializes the registry hash table)
  - dshash_find_or_insert (searches or creates registry entries)
  - dsm_create (creates new DSM segments)
  - dsm_pin_segment (pins segment in memory)
  - dsm_pin_mapping (pins segment mapping)
  - dsm_segment_handle (gets segment handle for storage)
  - dsm_segment_address (gets segment memory address)
  - dsm_find_mapping (finds existing segment mapping)
  - dsm_attach (attaches to existing segment)
  - dshash_release_lock (releases hash table entry lock)
  - DSMRegistryEntry (structure for registry entries)
- Called from (representative examples):
  - injection_init_shmem (in injection points test module)
  - tdr_attach_shmem (in DSM registry test module)

## Notes and Other Information
- This function switches to TopMemoryContext to ensure any local DSM/DSA allocations persist
- Segments are automatically pinned to prevent premature cleanup
- Size validation ensures existing segments match requested sizes exactly
- The function is designed to be safe for concurrent access from multiple backends
- Critical component of PostgreSQL's infrastructure for shared data structures like background worker communication and extension-specific shared state
- Error handling includes specific messages for common failure modes like empty names, oversized names, and size mismatches