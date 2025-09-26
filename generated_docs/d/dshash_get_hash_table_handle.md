# dshash_get_hash_table_handle

## Location
[src/backend/lib/dshash.c:367-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L367-L389)

## Overview
Returns a handle that allows other processes to attach to an existing dynamic shared hash table.

## Definition

```c
dshash_table_handle
dshash_get_hash_table_handle(dshash_table *hash_table)
```
## Detailed Description
The dshash_get_hash_table_handle function provides a simple accessor to retrieve the handle for a shared hash table. This handle is essentially a dsa_pointer that identifies the location of the hash table's control structure within the dynamic shared area. The handle can be passed to other processes or stored for later use, allowing them to attach to the same shared hash table using dshash_attach.

The function includes validation to ensure the hash table is still valid by checking the magic number. The returned handle remains valid until the hash table is destroyed, and can be used by multiple processes simultaneously to attach to the same shared hash table instance.

## Parameters / Member Variables
- : Pointer to the dshash_table structure from which to get the handle

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validates DSHASH_MAGIC)
- Called from (representative examples):
  - logicalrep_launcher_attach_dshmem
  - init_dsm_registry
  - StatsShmemInit
  - SharedRecordTypmodRegistryInit

## Notes and Other Information
- Returns the handle stored in the control structure during hash table creation
- The handle is essentially a dsa_pointer to the dshash_table_control structure
- Handle remains valid until the hash table is destroyed with dshash_destroy
- Multiple processes can use the same handle to attach to the hash table
- Commonly used immediately after dshash_create to store the handle for later access
- The handle can be passed between processes through shared memory or other IPC mechanisms