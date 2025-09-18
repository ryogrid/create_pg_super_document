# hash_initial_lookup

## Location
src/backend/utils/hash/dynahash.c: 1716 - 1739

## Overview
Performs the initial lookup operation to locate a hash bucket for a given hash value, returning both the bucket number and bucket pointer.

## Definition


## Detailed Description
This inline function implements the core bucket lookup mechanism for PostgreSQL's dynamic hash tables. It translates a hash value into the corresponding bucket location by first calculating the bucket number using the hash table's parameters, then determining which segment contains that bucket, and finally locating the specific bucket within that segment. The function uses bit shifting and modular arithmetic to efficiently map hash values to their storage locations in the segmented hash table structure.

The function includes corruption detection - if a required segment is found to be NULL, it calls hash_corrupted to handle the error condition. This ensures data integrity and helps detect hash table corruption early.

## Parameters / Member Variables
- : Pointer to the HTAB structure representing the hash table
- : The hash value for which to find the corresponding bucket
- : Output parameter that receives a pointer to the located hash bucket

## Dependencies
- Functions called/Symbols referenced:
  - [calc_bucket](../c/calc_bucket.md) (calculates bucket number from hash value)
  - MOD (modular arithmetic macro)
  - [hash_corrupted](hash_corrupted.md) (error handling for corrupted hash tables)
- Data structures referenced:
  - [HTAB](../H/HTAB.md) (hash table structure)
  - [HASHHDR](../H/HASHHDR.md) (hash table header)
  - HASHBUCKET (hash bucket structure)
  - HASHSEGMENT (hash table segment)
- Called from (representative examples):
  - [hash_search_with_hash_value](hash_search_with_hash_value.md)
  - [hash_update_hash_key](hash_update_hash_key.md)

## Notes and Other Information
- Declared as static inline for performance optimization
- Returns the calculated bucket number as the function result
- Uses bit shifting (sshift) and segment size (ssize) for efficient bucket-to-segment mapping
- The segment directory (hashp->dir) is indexed by segment number
- Critical for hash table performance as it's called for every hash operation
- Corruption detection helps maintain hash table integrity by catching NULL segments