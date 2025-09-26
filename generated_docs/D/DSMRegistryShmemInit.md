# DSMRegistryShmemInit

## Location
[src/backend/storage/ipc/dsm_registry.c:69-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_registry.c#L69-L90)

## Overview
DSMRegistryShmemInit initializes the shared memory structure for the DSM registry system that manages named dynamic shared memory segments.

## Definition
void DSMRegistryShmemInit(void)

## Detailed Description
DSMRegistryShmemInit is responsible for initializing the DSM registry's shared memory context during PostgreSQL startup. The function allocates and initializes the DSMRegistryCtx global variable, which serves as the control structure for the entire DSM registry system.

The function uses ShmemInitStruct to either create a new shared memory segment or attach to an existing one. If this is the first time the structure is being created (indicated by the 'found' flag being false), it initializes the DSA (Dynamic Shared Area) and DSHASH (Dynamic Shared Hash) handles to invalid values, indicating that these components haven't been set up yet.

## Parameters / Member Variables
- No parameters (void function)
- Uses local variable 'found' to determine if the shared memory structure already exists

## Dependencies
- Functions called/Symbols referenced:
  - [DSMRegistryCtxStruct](DSMRegistryCtxStruct.md) (structure type for the registry context)
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory initialization function)
  - [DSMRegistryShmemSize](DSMRegistryShmemSize.md) (calculates required memory size)
  - DSA_HANDLE_INVALID (constant for invalid DSA handle)
  - DSHASH_HANDLE_INVALID (constant for invalid DSHASH handle)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (during PostgreSQL startup)

## Notes and Other Information
- This function must be called during PostgreSQL startup after shared memory has been set up
- The DSA and DSHASH handles are initialized to invalid values and will be properly set up later by init_dsm_registry
- Part of the initialization sequence for PostgreSQL's dynamic shared memory registry infrastructure
- The 'found' parameter from ShmemInitStruct indicates whether this is a fresh initialization or attachment to existing shared memory