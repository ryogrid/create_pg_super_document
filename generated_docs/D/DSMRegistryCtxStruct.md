# DSMRegistryCtxStruct

## Location
[src/backend/storage/ipc/dsm_registry.c:35-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_registry.c#L35-L39)

## Overview
DSMRegistryCtxStruct represents the shared memory context for the dynamic shared memory registry, containing handles to both the dynamic shared area and the hash table used for storing named DSM segment entries.

## Definition

```c
typedef struct DSMRegistryCtxStruct
{
	dsa_handle	dsah;
	dshash_table_handle dshh;
} DSMRegistryCtxStruct;
```
## Detailed Description
This structure serves as the core context for PostgreSQL's dynamic shared memory registry system. The registry provides a way for libraries to use shared memory without needing to request it at startup time via a shmem_request_hook. The DSMRegistryCtxStruct maintains the necessary handles to access both the underlying dynamic shared area (DSA) and the distributed hash table that stores the actual registry entries keyed by library-specified strings.

The structure is used internally by the DSM registry implementation to maintain persistent access to the shared memory structures that store the mapping between named segments and their corresponding DSM handles.

## Parameters / Member Variables
- `dsah`: Handle to the dynamic shared area (DSA) that provides the underlying memory allocation infrastructure for the registry
- `dshh`: Handle to the distributed shared hash table that stores the actual DSMRegistryEntry structures keyed by segment names
## Dependencies
- Functions called/Symbols referenced:
  - dsa_handle
  - dshash_table_handle
- Called from (representative examples):
  - DSMRegistryShmemSize
  - DSMRegistryShmemInit

## Notes and Other Information
- This structure is used as a singleton context (DSMRegistryCtx) in the DSM registry implementation
- The registry enables libraries to create and access named shared memory segments without pre-allocation at startup
- The structure provides the foundation for thread-safe access to the registry across multiple PostgreSQL backends
- Located in src/backend/storage/ipc/dsm_registry.c at lines 35-39