# xl_hash_squeeze_page

## Location
[src/include/access/hash_xlog.h:159-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L159-L171)

## Overview
The xl_hash_squeeze_page struct represents the WAL record data for hash index squeeze page operations, which are used to reclaim space by moving tuples from a freed overflow page to other pages.

## Definition

```c
typedef struct xl_hash_squeeze_page
{
	BlockNumber prevblkno;
	BlockNumber nextblkno;
	uint16		ntups;
	bool		is_prim_bucket_same_wrt;	/* true if the page to which
											 * tuples are moved is same as
											 * primary bucket page */
	bool		is_prev_bucket_same_wrt;	/* true if the page to which
											 * tuples are moved is the page
											 * previous to the freed overflow
											 * page */
} xl_hash_squeeze_page;
```
## Detailed Description
This structure contains the necessary information to perform or replay a hash index squeeze page operation during WAL recovery. The squeeze page operation is part of PostgreSQL's hash index management, specifically for reclaiming space when overflow pages are freed. During this operation, tuples from a freed overflow page are moved to other pages in the hash bucket chain, and the freed page is removed from the chain.

The operation involves up to 6 backup blocks:
- Backup Blk 0: page containing tuples moved from freed overflow page
- Backup Blk 1: freed overflow page
- Backup Blk 2: page previous to the freed overflow page
- Backup Blk 3: page next to the freed overflow page
- Backup Blk 4: bitmap page containing info of freed overflow page
- Backup Blk 5: meta page

## Parameters / Member Variables
- : Block number of the page that was previous to the freed overflow page in the bucket chain
- : Block number of the page that was next to the freed overflow page in the bucket chain
- : Number of tuples that were moved from the freed overflow page
- : Boolean flag indicating whether the page receiving the moved tuples is the same as the primary bucket page
- : Boolean flag indicating whether the page receiving the moved tuples is the page that was previous to the freed overflow page

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - uint16 (type)
  - [bool](../b/bool.md) (type)
- Called from (representative examples):
  - [hash_xlog_squeeze_page](../h/hash_xlog_squeeze_page.md) (WAL replay function)
  - [_hash_freeovflpage](../h/_hash_freeovflpage.md) (hash overflow page freeing function)
  - [hash_desc](../h/hash_desc.md) (hash WAL record description function)
  - SizeOfHashSqueezePage (macro for calculating structure size)

## Notes and Other Information
- This is specifically used for XLOG_HASH_SQUEEZE_PAGE WAL record type
- The boolean flags help optimize the WAL replay process by indicating the relationship between pages involved in the operation
- Part of PostgreSQL's hash index access method implementation
- Critical for maintaining consistency during crash recovery of hash index operations
- Defined in src/include/access/hash_xlog.h at lines 159-171