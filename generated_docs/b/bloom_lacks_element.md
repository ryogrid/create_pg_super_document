# bloom_lacks_element

## Location
src/backend/lib/bloomfilter.c: 157 - 186

## Overview
Tests whether an element is definitely absent from the Bloom filter, providing probabilistic set membership testing with no false negatives.

## Definition
```c
bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem, size_t len)
```

## Detailed Description
The `bloom_lacks_element` function performs a membership test by computing the same k hash values used during element insertion and checking if all corresponding bits are set in the bitset. If any bit is unset, the element was definitely never added to the filter.

This function provides the core probabilistic membership testing capability of Bloom filters:
- True return value: Element is definitely NOT in the set (no false negatives)  
- False return value: Element is probably in the set (false positives possible)

The function uses the same bit manipulation techniques as `bloom_add_element` to efficiently map hash values to bit positions and test their state.

## Parameters / Member Variables
- `filter`: Pointer to the bloom_filter structure to query
- `elem`: Pointer to the element data to test for membership  
- `len`: Length in bytes of the element data

## Dependencies
- Functions called/Symbols referenced:
  - [k_hashes](../k/k_hashes.md): Computes k independent hash values for the element
  - `MAX_HASH_FUNCS`: Maximum number of hash functions supported
  - [bloom_filter](bloom_filter.md): The filter structure type

- Called from (representative examples):
  - `roles_list_append`: Testing for duplicate role additions in ACL processing
  - `nfalsepos_for_missing_strings`: Test validation of false positive rates

## Notes and Other Information
- Guarantees no false negatives - if element was added, function will return false
- May return false positives - elements never added might appear to be present
- Early termination optimization - returns true immediately when first unset bit is found
- Uses same bit addressing as bloom_add_element for consistency
- False positive rate depends on filter size, number of elements, and hash function count