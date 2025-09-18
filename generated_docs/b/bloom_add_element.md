# bloom_add_element

## Location
[src/backend/lib/bloomfilter.c:135-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bloomfilter.c#L135-L156)

## Overview
Adds an element to a Bloom filter by computing multiple hash values and setting the corresponding bits in the filter's bitset.

## Definition
```c
void bloom_add_element(bloom_filter *filter, unsigned char *elem, size_t len)
```

## Detailed Description
The `bloom_add_element` function inserts an element into the Bloom filter by computing k independent hash values for the input element and setting the corresponding bits in the bitset to 1. The number of hash functions (k) was determined during filter creation to optimize the false positive rate.

The function uses efficient bit manipulation to map hash values to specific bit positions within the byte-oriented bitset. Each hash value is converted from a bit address to a byte address with bit offset using bit shifting operations.

Once an element is added to the filter, subsequent calls to `bloom_lacks_element` for the same element will return false (indicating the element might be in the set), though false positives are possible for elements not actually added.

## Parameters / Member Variables
- `filter`: Pointer to the bloom_filter structure to modify
- `elem`: Pointer to the element data to be added to the filter
- `len`: Length in bytes of the element data

## Dependencies
- Functions called/Symbols referenced:
  - [k_hashes](../k/k_hashes.md): Computes k independent hash values for the element
  - `MAX_HASH_FUNCS`: Maximum number of hash functions supported
  - [bloom_filter](bloom_filter.md): The filter structure type

- Called from (representative examples):
  - `roles_list_append`: Adding role identifiers to ACL membership filter
  - `populate_with_dummy_strings`: Test data population

## Notes and Other Information
- Sets bits but never clears them - Bloom filters only support additions, not deletions
- Uses bit shifting (>> 3) to convert bit address to byte address (divide by 8)
- Uses bitwise AND with 7 to get bit offset within byte (equivalent to modulo 8)
- Multiple hash functions ensure good distribution and optimal false positive rate
- No bounds checking is performed - [hash](../h/hash.md) values must be within bitset range