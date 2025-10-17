# shared_record_table_hash

## Location
[src/backend/utils/cache/typcache.c:241-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L241-L277)

## Overview
A hash function for SharedRecordTableKey structures that generates consistent hash values based on the associated TupleDesc structure.

## Definition

```c
typedef struct RecordCacheArrayEntry
{
	uint64		id;
	TupleDesc	tupdesc;
} RecordCacheArrayEntry;
```
## Detailed Description
This function generates hash values for SharedRecordTableKey structures used in dynamic shared hash tables. It extracts the TupleDesc from the key (handling both shared and local variants) and delegates to hashRowType to compute the actual hash value. This ensures that SharedRecordTableKey structures representing equivalent row types will produce the same hash value, which is essential for proper hash table functionality.

The function works in conjunction with shared_record_table_compare to provide complete hash table key semantics - keys that compare as equal will always hash to the same value, maintaining hash table consistency.

## Parameters / Member Variables
- `a`: Pointer to the SharedRecordTableKey to hash
- `size`: Size parameter (unused in this implementation)
- `arg`: DSA area pointer used to dereference shared TupleDesc pointers

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md) (for resolving shared TupleDesc pointers)
  - [hashRowType](../h/hashRowType.md) (for computing the actual hash value)
- Called from (representative examples):
  - [Hash](../H/Hash.md) table operations (indirectly as callback function)

## Notes and Other Information
- This function is designed to work with PostgreSQL's dynamic shared memory hash tables
- It properly handles both shared and local TupleDesc references
- The hash value is computed using hashRowType, ensuring consistency with equalRowTypes comparison semantics
- The function is static and only used internally within the typcache.c module
- Returns a uint32 hash value suitable for hash table indexing

## Simplified Source

```c
static uint32 shared_record_table_hash(const void *a, size_t size, void *arg) {
    dsa_area *area = (dsa_area *) arg;
    SharedRecordTableKey *key = (SharedRecordTableKey *) a;
    TupleDesc tupdesc;

    // Extract TupleDesc from either shared or local memory
    if (key->shared) {
        tupdesc = (TupleDesc) dsa_get_address(area, key->u.shared_tupdesc);
    } else {
        tupdesc = key->u.local_tupdesc;
    }

    // Compute hash value using the TupleDesc
    return hashRowType(tupdesc);
}
```