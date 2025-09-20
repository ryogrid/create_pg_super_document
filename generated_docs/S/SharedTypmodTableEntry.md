# SharedTypmodTableEntry

## Location
[src/backend/utils/cache/typcache.c:205-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L205-L209)

## Overview
SharedTypmodTableEntry is a structure used in PostgreSQL's SharedRecordTypmodRegistry to enable reverse lookup of TupleDesc structures in shared memory using typmod values.

## Definition

```c
typedef struct SharedTypmodTableEntry
{
	uint32		typmod;
	dsa_pointer shared_tupdesc;
} SharedTypmodTableEntry;
```
## Detailed Description
SharedTypmodTableEntry serves as an entry in the SharedRecordTypmodRegistry's typmod table, providing a mapping mechanism from typmod values to their corresponding TupleDesc structures stored in shared memory. This structure is essential for the reverse lookup functionality in PostgreSQL's type cache system, allowing efficient retrieval of tuple descriptors when only the typmod identifier is known.

The structure supports PostgreSQL's shared memory optimization strategy by maintaining references to shared TupleDesc structures rather than duplicating the data across multiple backends. This approach significantly reduces memory usage and improves performance when multiple database processes need access to the same record type information.

## Parameters / Member Variables
- `typmod`: A 32-bit unsigned integer representing the type modifier identifier used as the lookup key
- `shared_tupdesc`: A dsa_pointer that references a TupleDesc structure stored in shared memory, enabling cross-backend access to the tuple descriptor
## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
- Called from (representative examples):
  - [shared_record_table_hash](../s/shared_record_table_hash.md)
  - [lookup_rowtype_tupdesc_internal](../l/lookup_rowtype_tupdesc_internal.md)
  - [SharedRecordTypmodRegistryInit](SharedRecordTypmodRegistryInit.md)
  - [find_or_make_matching_shared_tupledesc](../f/find_or_make_matching_shared_tupledesc.md)

## Notes and Other Information
- Located in src/backend/utils/cache/typcache.c:205-209
- Part of the SharedRecordTypmodRegistry system for efficient type cache management
- Enables reverse typmod-to-TupleDesc lookups in shared memory
- Uses DSA (Dynamic Shared Areas) pointers for cross-backend memory access
- Critical component for PostgreSQL's record type sharing optimization