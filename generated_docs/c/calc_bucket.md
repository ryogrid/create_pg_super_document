# calc_bucket

## Location
src/backend/utils/hash/dynahash.c: 919 - 955

## Overview
Converts a hash value to a bucket number using the hash table's masking scheme for dynamic bucket allocation.

## Definition


## Detailed Description
This internal function maps a hash value to the appropriate bucket number within the dynamic hash table structure. It uses a two-level masking approach to handle the table's dynamic expansion: first applying the high_mask, then conditionally applying the low_mask if the result exceeds the current maximum bucket. This allows the hash table to grow incrementally while maintaining proper distribution of entries.

## Parameters / Member Variables
- : Pointer to the HASHHDR structure containing the hash table control information
- : The hash value to be mapped to a bucket number

## Dependencies
- Functions called/Symbols referenced:
  - [HASHHDR](../H/HASHHDR.md) (hash table header structure)
- Called from (representative examples):
  - [expand_table](../e/expand_table.md)
  - [hash_initial_lookup](../h/hash_initial_lookup.md)

## Notes and Other Information
- Declared as static inline for performance optimization
- Returns a uint32 bucket number
- Uses bitwise AND operations with high_mask and low_mask for efficient bucket calculation
- The two-mask approach supports incremental table expansion without requiring full rehashing
- high_mask represents the mask for the expanded portion of the table
- low_mask is used for the original portion when the initial bucket calculation exceeds max_bucket
- Critical component of PostgreSQL's dynamic hashing algorithm