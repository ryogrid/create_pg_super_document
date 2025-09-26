# init_dsm_registry

## Location
[src/backend/storage/ipc/dsm_registry.c:91-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_registry.c#L91-L130)

## Overview
init_dsm_registry initializes or attaches to the dynamic shared hash table that stores DSM registry entries for managing named dynamic shared memory segments.

## Definition
static void init_dsm_registry(void)

## Detailed Description
init_dsm_registry is a critical internal function that sets up the dynamic shared hash table infrastructure for the DSM registry system. This function must be called before any access to the registry table and implements a lazy initialization pattern with proper concurrency control.

The function operates in two modes:
1. **Creation mode**: If no hash table handle exists (DSHASH_HANDLE_INVALID), it creates a new dynamic shared area (DSA) and hash table, pins them in memory, and stores the handles in shared memory for other backends to use.
2. **Attachment mode**: If handles already exist in shared memory, it attaches to the existing DSA and hash table created by another backend.

The function uses DSMRegistryLock to ensure thread-safe initialization, preventing race conditions when multiple backends attempt to initialize the registry simultaneously.

## Parameters / Member Variables
- No parameters (void function)
- Uses global variables:
  - : The dynamic shared hash table for registry entries
  - : The dynamic shared area for memory allocation
  - : Shared memory context containing handles

## Dependencies
- Functions called/Symbols referenced:
  - DSHASH_HANDLE_INVALID (constant for checking uninitialized handles)
  - LWLockAcquire/LWLockRelease (for concurrency control)
  - dsa_create (creates new dynamic shared area)
  - dsa_pin (pins DSA in memory)
  - dsa_pin_mapping (pins DSA mapping)
  - dshash_create (creates new dynamic shared hash table)
  - dsa_get_handle (gets handle for sharing)
  - dshash_get_hash_table_handle (gets hash table handle)
  - dsa_attach (attaches to existing DSA)
  - dshash_attach (attaches to existing hash table)
  - LWTRANCHE_DSM_REGISTRY_DSA (lock tranche for DSA operations)
- Called from (representative examples):
  - GetNamedDSMSegment (before accessing registry entries)

## Notes and Other Information
- This is a static function, only accessible within the dsm_registry.c file
- Implements lazy initialization - the hash table is only created when first needed
- Uses double-checked locking pattern for efficient concurrent access
- The DSA and hash table are pinned to prevent them from being detached while in use
- Critical for the proper functioning of PostgreSQL's named DSM segment infrastructure