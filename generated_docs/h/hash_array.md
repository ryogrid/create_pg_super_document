# hash_array

## Location
[src/backend/utils/adt/arrayfuncs.c:4146-4278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4146-L4278)

## Overview
PostgreSQL function that computes a hash value for an entire array by combining hash values of individual elements using a multiplicative hash algorithm.

## Definition


## Detailed Description
The  function calculates a hash value for an array by iterating through all elements and combining their individual hash values using a multiplicative hash algorithm. It uses a rolling hash technique where each element's hash is combined using the formula: , which is equivalent to .

The function handles special cases including NULL elements (treated as having hash value 0) and record types. For record types, it creates a fake type cache entry since the type cache doesn't consider records hashable by default, but commits to hashing them anyway.

The hash algorithm provides good distribution properties for arrays up to 2^27 elements, where each element's hash value is multiplied by a different odd number in the cyclic group formed by powers of 31 modulo 2^32.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Array to hash (accessed via )
  - Function call context and collation information

## Dependencies
- Functions called/Symbols referenced:
  -  - Get number of array dimensions
  -  - Get array dimension sizes
  -  - Get array element type OID
  -  - Get cached type information with hash function
  -  - Set up function manager info for record hashing
  -  - Calculate total number of array elements
  -  - [Initialize](../I/Initialize.md) array iterator
  -  - Get next array element
  -  - Call element hash function
  -  - Extract uint32 from Datum
  -  - Free detoasted array copies
  -  - Return hash result

- Called from (representative examples):
  - Used as hash support function for hash indexes on array columns
  - Called by hash-based operations and hash joins involving arrays

## Notes and Other Information
- Returns a 32-bit unsigned integer hash value
- Uses type cache to avoid repeated hash function lookups, improving performance for index operations
- NULL elements are assigned hash value 0 for consistent behavior
- Special handling for RECORD types by creating fake type cache entries
- Hash algorithm provides good distribution for arrays with up to 134 million elements (2^27)
- Multiplicative constant 31 is chosen for good hash distribution properties
- Handles toasted arrays properly by freeing detoasted copies to prevent memory leaks
- The hash result incorporates all array elements but not array metadata (dimensions, bounds)