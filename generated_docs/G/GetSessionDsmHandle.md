# GetSessionDsmHandle

## Location
[src/backend/access/common/session.c:70-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/session.c#L70-L154)

## Overview
Creates and initializes a per-session DSM (Dynamic Shared Memory) segment for sharing state between parallel worker processes, returning a handle that workers can use to attach to the segment.

## Definition
dsm_handle GetSessionDsmHandle(void)

## Detailed Description
GetSessionDsmHandle initializes the per-session DSM segment if it hasn't already been created, and returns its handle for worker processes to attach to. Unlike per-context DSM segments, this segment and its contents are reused across multiple parallel queries within the same session.

The function performs the following key operations:
1. Returns existing segment handle if already created
2. Estimates required space for DSA area and typmod registry
3. Creates DSM segment with estimated size
4. Sets up shared memory TOC (Table of Contents) with magic number
5. Creates session-scoped DSA area for dynamic allocation
6. Initializes shared record typmod registry for ephemeral row types
7. Pins both DSM segment and DSA area mappings to keep them alive
8. Updates CurrentSession with segment and area references

The function handles resource exhaustion gracefully by returning DSM_HANDLE_INVALID if a segment cannot be allocated due to lack of resources.

## Parameters / Member Variables
This function takes no parameters and returns:
- dsm_handle: A handle to the created DSM segment, or DSM_HANDLE_INVALID if allocation fails

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_segment_handle](../d/dsm_segment_handle.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - shm_toc_initialize_estimator
  - shm_toc_estimate_keys
  - shm_toc_estimate_chunk
  - [SharedRecordTypmodRegistryEstimate](../S/SharedRecordTypmodRegistryEstimate.md)
  - [shm_toc_estimate](../s/shm_toc_estimate.md)
  - [dsm_create](../d/dsm_create.md)
  - [shm_toc_create](../s/shm_toc_create.md)
  - [dsm_segment_address](../d/dsm_segment_address.md)
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - dsa_create_in_place
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [SharedRecordTypmodRegistryInit](../S/SharedRecordTypmodRegistryInit.md)
  - [dsm_pin_mapping](../d/dsm_pin_mapping.md)
  - [dsa_pin_mapping](../d/dsa_pin_mapping.md)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (in src/backend/access/transam/parallel.c:251)

## Notes and Other Information
- Creates a reusable DSM segment that persists across multiple parallel queries in the same session
- Uses SESSION_DSA_SIZE (0x30000 bytes) for the DSA area to avoid creating additional DSM segments in common cases
- The segment is pinned to remain mapped for the backend's lifetime
- Includes space estimation and allocation for shared record typmod registry used by ephemeral row types
- Returns DSM_HANDLE_INVALID on resource exhaustion rather than throwing an error
- All allocations are done in TopMemoryContext to ensure proper lifetime management