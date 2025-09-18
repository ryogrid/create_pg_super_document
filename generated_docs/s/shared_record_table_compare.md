# shared_record_table_compare

## Location
[src/backend/utils/cache/typcache.c:215-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L215-L240)

## Overview
A comparator function for SharedRecordTableKey structures that determines equality between two keys by comparing their associated TupleDesc structures.

## Definition


## Detailed Description
This function serves as a comparison callback for hash table operations involving SharedRecordTableKey structures. It extracts TupleDesc pointers from two SharedRecordTableKey structures and uses the equalRowTypes function to determine if they represent equivalent row types. The function handles both shared (stored in dynamic shared memory) and local TupleDesc structures, properly dereferencing shared pointers using the provided DSA area.

The comparison is binary - it returns 0 for equal keys and 1 for unequal keys, which is the expected behavior for hash table comparator functions that need to distinguish between equivalent and different keys.

## Parameters / Member Variables
- `a`: Pointer to the first SharedRecordTableKey to compare
- `b`: Pointer to the second SharedRecordTableKey to compare  
- `size`: Size parameter (unused in this implementation)
- `arg`: DSA area pointer used to dereference shared TupleDesc pointers

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md) (for resolving shared TupleDesc pointers)
  - [equalRowTypes](../e/equalRowTypes.md) (for comparing TupleDesc structures)
- Called from (representative examples):
  - [shared_record_table_hash](shared_record_table_hash.md) (indirectly as part of hash table operations)

## Notes and Other Information
- This function is designed to work with PostgreSQL's dynamic shared memory architecture
- It properly handles the mixed case where one key might reference a shared TupleDesc while another references a local one
- The function is static and only used internally within the typcache.c module
- Returns 0 for equality and 1 for inequality, following standard C comparison function conventions for hash tables