# AttachSession

## Location
src/backend/access/common/session.c: 155 - 200

## Overview
Attaches a worker process to an existing per-session DSM segment created by the parallel leader process, enabling access to shared session state.

## Definition
void AttachSession(dsm_handle handle)

## Detailed Description
AttachSession allows worker processes to attach to a per-session DSM segment that was previously created by the parallel leader using GetSessionDsmHandle(). This function establishes the worker's connection to shared session resources including the DSA area and shared record typmod registry.

The function performs the following operations:
1. Attaches to the DSM segment using the provided handle
2. Locates and attaches to the shared memory TOC using the session magic number
3. Finds and attaches to the DSA area for dynamic shared memory allocation
4. Updates CurrentSession with segment and area references
5. Attaches to the shared record typmod registry for ephemeral row types
6. Pins both DSM segment and DSA area mappings to keep them alive until DetachSession() or backend exit

The function throws an ERROR if it cannot attach to the DSM segment, ensuring that worker processes don't continue with an invalid session state.

## Parameters / Member Variables
- handle: dsm_handle of the session DSM segment created by the parallel leader

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - dsm_attach
  - elog
  - shm_toc_attach
  - dsm_segment_address
  - shm_toc_lookup
  - dsa_attach_in_place
  - SharedRecordTypmodRegistryAttach
  - dsm_pin_mapping
  - dsa_pin_mapping
- Called from (representative examples):
  - ParallelWorkerMain (in src/backend/access/transam/parallel.c:1464)

## Notes and Other Information
- Must be called by worker processes to access shared session state
- The DSM segment handle is typically passed from leader to worker through parallel query infrastructure
- Validates the DSM segment using SESSION_MAGIC to ensure proper attachment
- Pins the mappings to prevent detachment until explicitly called or backend termination
- All operations are performed in TopMemoryContext for proper lifetime management
- Throws ERROR on attachment failure rather than returning an error code
- The attached session remains available until DetachSession() is called or the backend exits