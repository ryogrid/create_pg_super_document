# xl_hash_add_ovfl_page

## Location
[src/include/access/hash_xlog.h:80-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L80-L84)

## Overview
A PostgreSQL WAL record structure that captures the information needed to replay the addition of an overflow page to a hash index bucket chain during recovery.

## Definition

```c
typedef struct xl_hash_add_ovfl_page
{
	uint16		bmsize;
	bool		bmpage_found;
} xl_hash_add_ovfl_page;
```
## Detailed Description
The  structure is used for  WAL records, which log the addition of overflow pages to hash index bucket chains when the primary bucket page becomes full. This operation is more complex than simple insertions as it involves managing the bucket chain structure and potentially allocating new bitmap pages to track the overflow pages.

The record works with up to five backup blocks that capture the complete state needed for recovery:
- Backup Block 0: The newly allocated overflow page
- Backup Block 1: The page that precedes the new overflow page in the bucket chain
- Backup Block 2: The bitmap page (if it exists)
- Backup Block 3: A new bitmap page (if one was allocated)
- Backup Block 4: The metapage containing index metadata

This structure stores metadata about the bitmap management aspects of the operation, which are crucial for properly maintaining the hash index's space management during WAL replay.

## Parameters / Member Variables
- : The size of the bitmap in pages, indicating how many pages the current bitmap can track
- : Boolean flag indicating whether an existing bitmap page was found and used, or if a new bitmap page needed to be allocated

## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
  - [bool](../b/bool.md) (type)
- Called from (representative examples):
  - [hash_xlog_add_ovfl_page](../h/hash_xlog_add_ovfl_page.md) (WAL replay function)
  - [_hash_addovflpage](../h/_hash_addovflpage.md) (overflow page addition implementation)
  - [hash_desc](../h/hash_desc.md) (WAL record description function)
  - SizeOfHashAddOvflPage (macro for size calculation)

## Notes and Other Information
- This operation involves complex space management including bitmap page allocation
- The boolean flag  is critical for determining whether bitmap initialization is needed during replay
- Part of PostgreSQL's hash index overflow page management system
- Defined in src/include/access/hash_xlog.h:80-84
- The operation may involve up to 5 different backup blocks, making it one of the more complex hash index WAL records
- Used when bucket pages exceed their capacity and require chaining to overflow pages