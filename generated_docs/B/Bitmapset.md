# Bitmapset

## Location
[src/include/nodes/bitmapset.h:49-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/bitmapset.h#L49-L56)

## Overview
Bitmapset is a PostgreSQL data structure that represents a set of nonnegative integers using a bitmap implementation, optimized for sets where the maximum value is relatively small.

## Definition


## Detailed Description
Bitmapset is PostgreSQL's generic bitmap set implementation that can represent any set of nonnegative integers. It is primarily designed for sets where the maximum value is not large (typically a few hundred at most). The structure uses a flexible array of bitmap words to efficiently store and manipulate sets of integers.

The implementation uses either 32-bit or 64-bit words depending on the platform's pointer size, with 64-bit words used when  for better performance on 64-bit systems. Each bit position in the bitmap corresponds to an integer value, with set bits indicating membership in the set.

By convention, an empty set is always represented by a NULL pointer rather than an allocated Bitmapset structure, which provides memory efficiency for the common case of empty sets.

## Parameters / Member Variables
- : NodeTag identifying this as a Bitmapset node type
- : Number of bitmap words in the words array, determining the capacity of the set
- : Flexible array member containing the actual bitmap data, with each word storing multiple bits representing set membership

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - bitmapword (typedef for uint32 or uint64 depending on platform)
  - NodeTag
- Called from (representative examples):
  - Various bitmapset manipulation functions (bms_add_member, bms_union, bms_intersect, etc.)
  - [Query](../Q/Query.md) planning and optimization code throughout PostgreSQL

## Notes and Other Information
- Empty sets are represented as NULL pointers, never as allocated Bitmapset structures
- The bitmap word size (32 or 64 bits) is automatically selected based on platform architecture
- Includes node attributes for custom copy/equal functions and special read/write handling
- Supports comprehensive set operations including union, intersection, difference, subset testing
- Provides iteration support through bms_next_member and bms_prev_member functions
- Can be used as hashtable keys with dedicated hash and comparison functions
- Maximum practical set size is limited by memory and intended for relatively small integer sets