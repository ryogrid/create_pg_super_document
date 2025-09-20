# xl_hash_split_complete

## Location
[src/include/access/hash_xlog.h:117-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L117-L121)

## Overview
A PostgreSQL WAL record structure that captures the information needed to replay the completion phase of a hash index bucket split operation during recovery.

## Definition

```c
typedef struct xl_hash_split_complete
{
	uint16		old_bucket_flag;
	uint16		new_bucket_flag;
} xl_hash_split_complete;
```
## Detailed Description
The  structure is used for  WAL records, which log the final completion phase of hash index bucket splitting. This represents the second phase of the bucket split process, which occurs after the initial page allocation handled by .

This operation finalizes the bucket split by ensuring both the old and new bucket pages are properly configured and marked with the correct status flags. It represents the point at which the split operation is considered complete and the hash index structure is consistent.

The record works with two backup blocks:
- Backup Block 0: The page for the old bucket (with finalized status)
- Backup Block 1: The page for the new bucket (with finalized status)

Unlike the allocation phase, this completion phase focuses on finalizing the bucket page states rather than managing metadata or performing tuple redistribution.

## Parameters / Member Variables
- `old_bucket_flag`: Flag indicating the final status/properties of the old bucket page after the split completion
- `new_bucket_flag`: Flag indicating the final status/properties of the new bucket page after the split completion
## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
- Called from (representative examples):
  - [hash_xlog_split_complete](../h/hash_xlog_split_complete.md) (WAL replay function)
  - [_hash_splitbucket](../h/_hash_splitbucket.md) (bucket splitting implementation)
  - [hash_desc](../h/hash_desc.md) (WAL record description function)
  - SizeOfHashSplitComplete (macro for size calculation)

## Notes and Other Information
- This is the final phase of a multi-phase bucket split operation in hash indexes
- Works as a pair with  to complete the full split process
- The flags ensure both bucket pages are left in a consistent, valid state
- Part of PostgreSQL's hash index dynamic expansion mechanism
- Defined in src/include/access/hash_xlog.h:117-121
- Simpler than the allocation phase as it primarily focuses on status finalization
- Critical for maintaining hash index consistency across the split operation boundary
- Used during crash recovery to ensure split operations are either fully completed or properly rolled back