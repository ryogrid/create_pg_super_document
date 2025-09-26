# CopyIndexTuple

## Location
[src/backend/access/common/indextuple.c:547-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L547-L575)

## Overview
Creates a palloc'd copy of an index tuple, providing memory allocation and duplication functionality for IndexTuple structures in PostgreSQL's indexing system.

## Definition

```c
IndexTuple
CopyIndexTuple(IndexTuple source)
```
## Detailed Description
The  function creates a complete copy of an existing IndexTuple by allocating new memory and copying all data from the source tuple. This function is essential for scenarios where index tuples need to be duplicated, such as during index operations that require preserving original tuples while creating modified versions, or when tuples need to be moved between different memory contexts.

The function determines the exact size of the source tuple using , allocates the appropriate amount of memory using , and performs a byte-by-byte copy using . This ensures that all tuple data, including the tuple header and attribute values, are faithfully reproduced in the new tuple.

## Parameters / Member Variables
- : The original IndexTuple to be copied. Must be a valid IndexTuple pointer.

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize
  - [palloc](../p/palloc.md)
  - memcpy

- Called from (representative examples):
  - [index_truncate_tuple](../i/index_truncate_tuple.md)
  - [gistformdownlink](../g/gistformdownlink.md)
  - [_hash_squeezebucket](../h/_hash_squeezebucket.md)
  - [_hash_splitbucket](../h/_hash_splitbucket.md)
  - [_bt_swap_posting](../b/_bt_swap_posting.md)
  - [_bt_insertonpg](../b/_bt_insertonpg.md)
  - [_bt_insert_parent](../b/_bt_insert_parent.md)
  - [_bt_newlevel](../b/_bt_newlevel.md)
  - [_bt_pagedel](../b/_bt_pagedel.md)
  - [_bt_buildadd](../b/_bt_buildadd.md)
  - [_bt_load](../b/_bt_load.md)
  - [btree_xlog_insert](../b/btree_xlog_insert.md)
  - [btree_xlog_split](../b/btree_xlog_split.md)

## Notes and Other Information
- The function allocates memory in the current memory context using , so the caller is responsible for ensuring proper memory management
- The copied tuple is completely independent of the source tuple and can be modified without affecting the original
- This function is widely used across different index access methods (B-tree, GiST, Hash) demonstrating its fundamental role in PostgreSQL's indexing infrastructure
- The function performs a shallow copy of the tuple data, which is appropriate since IndexTuples contain primitive data types and fixed-size structures
- Located in src/backend/access/common/indextuple.c at lines 547-575