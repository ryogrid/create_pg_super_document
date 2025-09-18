# hash_aclitem_extended

## Location
src/backend/utils/adt/acl.c: 782 - 802

## Overview
A PostgreSQL system function that computes a 64-bit hash value for AclItem structures with an optional seed parameter, providing enhanced hashing capabilities for advanced hash operations.

## Definition
```c
Datum hash_aclitem_extended(PG_FUNCTION_ARGS)
```

## Detailed Description
The hash_aclitem_extended function is an enhanced version of hash_aclitem that provides 64-bit hash computation with seed support. This function is part of PostgreSQL's extended hash function family, which enables more sophisticated hashing operations such as hash joins with different hash seeds and other advanced database operations that require seeded hash functions.

Like its simpler counterpart, it uses an additive hash approach, summing the three numeric fields of the AclItem structure. However, it provides two modes of operation: when the seed is zero, it returns the simple sum as a 64-bit value; when a non-zero seed is provided, it delegates to hash_uint32_extended for more sophisticated hash computation with the seed.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: AclItem structure to hash (accessed via PG_GETARG_ACLITEM_P(0))
  - Argument 1: 64-bit seed value for hash computation (accessed via PG_GETARG_INT64(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACLITEM_P (macro to extract AclItem argument)
  - PG_GETARG_INT64 (macro to extract 64-bit integer seed)
  - [hash_uint32_extended](hash_uint32_extended.md) (function for extended hash computation with seed)
  - [UInt64GetDatum](../U/UInt64GetDatum.md) (macro to return 64-bit unsigned integer as Datum)
  - AclItem (structure type)
- Called from (representative examples):
  - Advanced hash operations requiring seeded hash functions
  - Hash joins and other database operations needing extended hash support

## Notes and Other Information
- Extends the basic hash_aclitem function with seed support and 64-bit output
- Uses conditional logic: simple sum for zero seed, extended hash for non-zero seed
- The seed parameter enables consistent hash computation across different contexts
- Part of PostgreSQL's extended hash function infrastructure for advanced database operations
- Returns a 64-bit hash value wrapped in a Datum for PostgreSQL's type system
- The function signature follows PostgreSQL's V1 calling convention for system functions
- Maintains the same simple additive hash base as hash_aclitem but with enhanced capabilities