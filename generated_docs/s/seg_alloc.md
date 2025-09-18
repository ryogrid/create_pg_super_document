# seg_alloc

## Location
[src/backend/utils/hash/dynahash.c:1647-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1647-L1665)

## Overview
Allocates and initializes a new hash table segment containing the specified number of hash buckets.

## Definition


## Detailed Description
The seg_alloc function allocates memory for a new hash table segment and initializes it with zeroed hash buckets. It uses the hash table's configured memory allocator and sets the appropriate memory context before allocation. The function allocates space for the number of buckets specified by the hash table's ssize (segment size) parameter and ensures all buckets are properly initialized to NULL/zero values. This function is essential for hash table growth, providing clean segments that can be added to the hash table's directory structure.

## Parameters / Member Variables
- : Pointer to the HTAB (hash table) structure for which a new segment should be allocated

## Dependencies
- Functions called/Symbols referenced:
  - HASHSEGMENT (type)
  - HASHBUCKET (type for size calculation)
  - MemSet
  - CurrentDynaHashCxt (global variable)
- Called from (representative examples):
  - [init_htab](../i/init_htab.md)
  - [expand_table](../e/expand_table.md)

## Notes and Other Information
- Returns a pointer to the allocated HASHSEGMENT on success, NULL on failure
- This is a static function, only used internally within dynahash.c
- Sets CurrentDynaHashCxt before allocation to ensure proper memory context
- Allocates space for hashp->ssize number of HASHBUCKET structures
- Initializes all allocated memory to zero using MemSet
- Used during both initial hash table creation and dynamic expansion
- Part of the PostgreSQL dynamic hash table segment management system