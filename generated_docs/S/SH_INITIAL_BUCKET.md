# SH_INITIAL_BUCKET

## Location
[src/include/lib/simplehash.h:357-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L357-L363)

## Overview
Computes the optimal starting bucket for a hash value in a PostgreSQL simple hash table using efficient bitwise AND masking.

## Definition

```c
static inline uint32
SH_INITIAL_BUCKET(SH_TYPE * tb, uint32 hash)
```
## Detailed Description
SH_INITIAL_BUCKET is a macro that generates a function name for computing the initial bucket position within PostgreSQL's simple hash table framework. The function takes a hash value and maps it to a valid bucket index by performing a bitwise AND operation with the table's size mask. This approach is highly efficient because the hash table size is always a power of 2, making the sizemask (size - 1) effectively perform a modulo operation through bitwise arithmetic. This function is fundamental to the hash table's addressing mechanism and is used extensively throughout hash table operations.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure containing the sizemask
- `hash`: The 32-bit hash value to map to a bucket

## Dependencies
- Functions called/Symbols referenced:
  - tb->sizemask (bitmask for efficient modulo operations)
- Called from (representative examples):
  - [SH_GROW](SH_GROW.md) (when redistributing elements during table growth)
  - [SH_INSERT_HASH_INTERNAL](SH_INSERT_HASH_INTERNAL.md) (when inserting new elements)
  - [SH_LOOKUP_HASH_INTERNAL](SH_LOOKUP_HASH_INTERNAL.md) (when searching for elements)
  - [SH_DELETE](SH_DELETE.md) (when removing elements)
  - [SH_DELETE_ITEM](SH_DELETE_ITEM.md) (when removing specific items)
  - [SH_STAT](SH_STAT.md) (when computing statistics)

## Notes and Other Information
- Returns hash & tb->sizemask, which is equivalent to hash % tb->size but much faster
- The sizemask is always (size - 1) where size is a power of 2, enabling bitwise optimization
- This function determines the starting point for probing in the hash table
- Part of PostgreSQL's templated simple hash table implementation where SH_PREFIX defines the specific hash table type
- Critical for hash table performance as it's called for every hash table operation