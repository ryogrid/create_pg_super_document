# dshash_parameters

## Location
[src/include/lib/dshash.h:54-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/dshash.h#L54-L62)

## Overview
The dshash_parameters struct defines the configuration parameters needed to create or attach to a dynamic shared hash table, specifying key and entry sizes, callback functions, and locking configuration.

## Definition

```c
typedef struct dshash_parameters
{
	size_t		key_size;		/* Size of the key (initial bytes of entry) */
	size_t		entry_size;		/* Total size of entry */
	dshash_compare_function compare_function;	/* Compare function */
	dshash_hash_function hash_function; /* Hash function */
	dshash_copy_function copy_function; /* Copy function */
	int			tranche_id;		/* The tranche ID to use for locks */
} dshash_parameters;
```
## Detailed Description
The dshash_parameters structure encapsulates all the configuration information required to create or attach to a dynamic shared hash table. This structure defines the memory layout, callback functions for key operations, and locking behavior. The compare, hash, and copy functions must be supplied even when attaching to an existing hash table because function pointers cannot be safely shared between backends. The user data pointer supplied to create and attach functions will be passed to these callback functions. When attaching to an existing hash table, the tranche_id member does not need to be initialized.

## Parameters / Member Variables
- `key_size`: Size in bytes of the key portion of each hash table entry (initial bytes)
- `entry_size`: Total size in bytes of each complete hash table entry
- `compare_function`: Function pointer for comparing two keys (signature: int (*)(const void *a, const void *b, size_t size, void *arg))
- `hash_function`: Function pointer for computing hash values from keys (signature: dshash_hash (*)(const void *v, size_t size, void *arg))
- `copy_function`: Function pointer for copying keys (signature: void (*)(void *dest, const void *src, size_t size, void *arg))
- `tranche_id`: Integer identifier for the lock tranche to use for hash table locking
## Dependencies
- Functions called/Symbols referenced:
  - dshash_compare_function (function type)
  - dshash_hash_function (function type)
  - dshash_copy_function (function type)
- Called from (representative examples):
  - dshash_create
  - dshash_attach
  - [shared_record_table_hash](../s/shared_record_table_hash.md)

## Notes and Other Information
- Function pointers must be provided even when attaching because they cannot be safely shared between backends
- The tranche_id only needs to be set when creating new hash tables, not when attaching
- Common convenience functions are available: dshash_memcmp/dshash_memhash/dshash_memcpy for binary data, dshash_strcmp/dshash_strhash/dshash_strcpy for string data
- The structure is defined in src/include/lib/dshash.h:54-62