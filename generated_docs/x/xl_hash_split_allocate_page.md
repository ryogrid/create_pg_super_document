# xl_hash_split_allocate_page

## Location
[src/include/access/hash_xlog.h:98-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L98-L104)

## Overview
A PostgreSQL WAL record structure that captures the information needed to replay the page allocation phase of a hash index bucket split operation during recovery.

## Definition

```c
typedef struct xl_hash_split_allocate_page
{
	uint32		new_bucket;
	uint16		old_bucket_flag;
	uint16		new_bucket_flag;
	uint8		flags;
} xl_hash_split_allocate_page;
```
## Detailed Description
The  structure is used for  WAL records, which log the page allocation phase of hash index bucket splitting. When a hash index grows and needs to split a bucket, this is the first phase where pages are allocated for both the old and new buckets before the actual tuple redistribution occurs.

This operation is part of the hash index expansion process that happens when the index grows beyond its current capacity. The split operation involves multiple phases, with this record handling the initial page allocation and setup phase.

The record works with three backup blocks:
- Backup Block 0: The page for the old bucket 
- Backup Block 1: The page for the new bucket
- Backup Block 2: The metapage containing updated index metadata

The structure stores the essential metadata needed to properly set up both bucket pages and maintain the hash index's structural integrity during recovery.

## Parameters / Member Variables
- : The bucket number for the newly created bucket that results from the split operation
- : Flag indicating the status/properties of the old bucket page (specific flag meanings depend on implementation context)
- : Flag indicating the status/properties of the new bucket page being allocated
- : General operation flags that control various aspects of the split allocation process

## Dependencies
- Functions called/Symbols referenced:
  - uint32 (type)
  - uint16 (type) 
  - uint8 (type)
- Called from (representative examples):
  - [hash_xlog_split_allocate_page](../h/hash_xlog_split_allocate_page.md) (WAL replay function)
  - [_hash_expandtable](../h/_hash_expandtable.md) (hash table expansion implementation)
  - [hash_desc](../h/hash_desc.md) (WAL record description function)
  - SizeOfHashSplitAllocPage (macro for size calculation)

## Notes and Other Information
- This is the first phase of a multi-phase bucket split operation in hash indexes
- The split process is triggered when the hash index needs to expand its capacity
- Both old and new bucket flags allow fine-grained control over page properties during the split
- Part of PostgreSQL's hash index dynamic expansion mechanism
- Defined in src/include/access/hash_xlog.h:98-104
- The operation coordinates page allocation with metadata updates to maintain index consistency
- Works in conjunction with other split-related WAL records like 