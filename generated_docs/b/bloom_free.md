# bloom_free

## Location
src/backend/lib/bloomfilter.c: 126 - 134

## Overview
Frees the memory allocated for a Bloom filter data structure, releasing both the filter header and its associated bitset.

## Definition
```c
void bloom_free(bloom_filter *filter)
```

## Detailed Description
The `bloom_free` function deallocates a Bloom filter that was previously created with `bloom_create`. Since the filter and its bitset are allocated as a single contiguous memory block, a single `pfree` call releases all associated memory.

This function provides proper cleanup for Bloom filter resources and should be called when the filter is no longer needed to prevent memory leaks.

## Parameters / Member Variables
- `filter`: Pointer to the bloom_filter structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation function
  - [bloom_filter](bloom_filter.md): The filter structure type

- Called from (representative examples):
  - `roles_is_member_of`: ACL role membership checking cleanup
  - `create_and_test_bloom`: Test module cleanup

## Notes and Other Information
- Simple wrapper around pfree() since bloom_create allocates filter and bitset as single memory block
- Must be paired with bloom_create to prevent memory leaks
- No validation is performed on the filter pointer - caller must ensure validity