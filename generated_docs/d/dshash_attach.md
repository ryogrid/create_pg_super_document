# dshash_attach

## Location
[src/backend/lib/dshash.c:270-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L270-L306)

## Overview
Attaches to an existing dynamic shared hash table using a handle, creating a backend-local representation for accessing the shared hash table data.

## Definition

```c
struct. */
	hash_table->area = area;
```
## Detailed Description
The dshash_attach function creates a backend-local dshash_table object that provides access to an existing shared hash table identified by the given handle. Unlike dshash_create, this function does not create new shared structures but rather connects to pre-existing ones. The function sets up the local representation with the provided parameters and establishes the connection to the shared control structure.

The bucket pointers and size information are initially set to safe defaults (NULL and 0) and will be properly initialized later by ensure_valid_bucket_pointers() when the hash table is actually accessed. This lazy initialization approach provides thread-safe access coordination through partition locking.

## Parameters / Member Variables
- : Dynamic shared area containing the existing hash table
- : Configuration parameters that must match the original hash table's parameters (hash function, comparison function, key size, entry size, tranche ID)
- : Handle to the existing shared hash table control structure (obtained from dshash_get_hash_table_handle)
- : User-provided context argument that will be passed to hash, compare, and copy functions

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [dsa_get_address](dsa_get_address.md)
  - Assert (validates DSHASH_MAGIC)
- Called from (representative examples):
  - [logicalrep_launcher_attach_dshmem](../l/logicalrep_launcher_attach_dshmem.md)
  - [init_dsm_registry](../i/init_dsm_registry.md)
  - [pgstat_attach_shmem](../p/pgstat_attach_shmem.md)
  - [SharedRecordTypmodRegistryAttach](../S/SharedRecordTypmodRegistryAttach.md)

## Notes and Other Information
- The function validates that the control structure has the correct magic number (DSHASH_MAGIC)
- Bucket pointers are intentionally left as NULL until first access for thread safety
- The handle parameter is essentially a dsa_pointer to the control structure
- Parameters must exactly match those used when the hash table was originally created
- This is used when multiple processes need to access the same shared hash table instance