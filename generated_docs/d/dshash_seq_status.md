# dshash_seq_status

## Location
[src/include/lib/dshash.h:72-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/dshash.h#L72-L81)

## Overview
The dshash_seq_status struct maintains state information for sequential scanning through a dynamic shared hash table, tracking current position, bucket information, and locking mode.

## Definition

```c
typedef struct dshash_seq_status
{
	dshash_table *hash_table;	/* dshash table working on */
	int			curbucket;		/* bucket number we are at */
	int			nbuckets;		/* total number of buckets in the dshash */
	dshash_table_item *curitem; /* item we are currently at */
	dsa_pointer pnextitem;		/* dsa-pointer to the next item */
	int			curpartition;	/* partition number we are at */
	bool		exclusive;		/* locking mode */
} dshash_seq_status;
```
## Detailed Description
The dshash_seq_status structure encapsulates all state information needed to perform sequential scanning through a dynamic shared hash table. While the implementation details are exposed to allow users to know the storage size requirements, this structure should be treated as opaque by callers and only manipulated through the provided dshash sequential scan API functions. The structure tracks the current scanning position at multiple levels: partition, bucket, and individual item level, along with maintaining references to the hash table being scanned and the locking mode being used.

## Parameters / Member Variables
- `*hash_table`: Pointer to the dshash_table being sequentially scanned
- `curbucket`: Current bucket number being scanned within the hash table
- `nbuckets`: Total number of buckets in the dynamic shared hash table
- `*curitem`: Pointer to the dshash_table_item currently being examined
- `pnextitem`: DSA pointer to the next item in the scan sequence
- `curpartition`: Current partition number being scanned (for partitioned tables)
- `exclusive`: Boolean flag indicating whether exclusive locking mode is being used
## Dependencies
- Functions called/Symbols referenced:
  - [dshash_table](dshash_table.md)
  - [dshash_table_item](dshash_table_item.md)
  - dsa_pointer
- Called from (representative examples):
  - dshash_seq_init
  - dshash_seq_next
  - dshash_seq_term
  - dshash_delete_current

## Notes and Other Information
- Although the structure members are exposed, callers should treat this as an opaque type and only use the provided API functions
- The structure is designed to efficiently track position across the multi-level organization of hash buckets and partitions
- Used extensively in PostgreSQL statistics system for scanning shared hash tables
- The exclusive flag determines the locking behavior during the scan operation
- The structure is defined in src/include/lib/dshash.h:72-81