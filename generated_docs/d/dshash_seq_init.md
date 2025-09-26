# dshash_seq_init

## Location
[src/backend/lib/dshash.c:638-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L638-L656)

## Overview
Initializes a sequential scan status structure for iterating through all elements in a dynamic shared hash table.

## Definition

```c
void
dshash_seq_init(dshash_seq_status *status, dshash_table *hash_table,
				bool exclusive)
```
## Detailed Description
dshash_seq_init prepares a dshash_seq_status structure for sequential scanning through a dynamic shared hash table. This function sets up the initial state for iteration, allowing the caller to traverse all elements in the hash table one by one using subsequent calls to dshash_seq_next(). The function supports both shared and exclusive scanning modes, where exclusive mode allows safe deletion of elements during iteration using dshash_delete_current().

## Parameters / Member Variables
- : Pointer to a dshash_seq_status structure that will track the scan state
- : The dynamic shared hash table to be scanned
- : Boolean flag indicating whether the scan should be exclusive (allowing safe deletion during iteration)

## Dependencies
- Functions called/Symbols referenced:
  - [dshash_table](dshash_table.md) (hash table structure)
  - [dshash_seq_status](dshash_seq_status.md) (scan status structure)
  - InvalidDsaPointer (DSA pointer constant)
- Called from (representative examples):
  - [pgstat_build_snapshot](../p/pgstat_build_snapshot.md) (src/backend/utils/activity/pgstat.c:999)
  - [pgstat_write_statsfile](../p/pgstat_write_statsfile.md) (src/backend/utils/activity/pgstat.c:1390)
  - [pgstat_drop_database_and_contents](../p/pgstat_drop_database_and_contents.md) (src/backend/utils/activity/pgstat_shmem.c:887)
  - [pgstat_drop_all_entries](../p/pgstat_drop_all_entries.md) (src/backend/utils/activity/pgstat_shmem.c:977)
  - [pgstat_reset_matching_entries](../p/pgstat_reset_matching_entries.md) (src/backend/utils/activity/pgstat_shmem.c:1036)

## Notes and Other Information
- This function must be paired with dshash_seq_term() to properly terminate the scan
- When exclusive=true, elements can be safely deleted during iteration using dshash_delete_current()
- The function initializes all scan state fields to their starting values (bucket 0, no current item, etc.)
- Primarily used in PostgreSQL statistics system for iterating through shared statistics data
- The scan state tracks the current bucket, partition, and item position within the hash table