# CopyIndexTuple

## Location
src/backend/access/common/indextuple.c: 547 - 575

## Overview
Creates a palloc'd copy of an index tuple, providing memory allocation and duplication functionality for IndexTuple structures in PostgreSQL's indexing system.

## Definition


## Detailed Description
The  function creates a complete copy of an existing IndexTuple by allocating new memory and copying all data from the source tuple. This function is essential for scenarios where index tuples need to be duplicated, such as during index operations that require preserving original tuples while creating modified versions, or when tuples need to be moved between different memory contexts.

The function determines the exact size of the source tuple using , allocates the appropriate amount of memory using , and performs a byte-by-byte copy using . This ensures that all tuple data, including the tuple header and attribute values, are faithfully reproduced in the new tuple.

## Parameters / Member Variables
- : The original IndexTuple to be copied. Must be a valid IndexTuple pointer.

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize
  - palloc
  - memcpy

- Called from (representative examples):
  - index_truncate_tuple
  - gistformdownlink
  - _hash_squeezebucket
  - _hash_splitbucket
  - _bt_swap_posting
  - _bt_insertonpg
  - _bt_insert_parent
  - _bt_newlevel
  - _bt_pagedel
  - _bt_buildadd
  - _bt_load
  - btree_xlog_insert
  - btree_xlog_split

## Notes and Other Information
- The function allocates memory in the current memory context using , so the caller is responsible for ensuring proper memory management
- The copied tuple is completely independent of the source tuple and can be modified without affecting the original
- This function is widely used across different index access methods (B-tree, GiST, Hash) demonstrating its fundamental role in PostgreSQL's indexing infrastructure
- The function performs a shallow copy of the tuple data, which is appropriate since IndexTuples contain primitive data types and fixed-size structures
- Located in src/backend/access/common/indextuple.c at lines 547-575