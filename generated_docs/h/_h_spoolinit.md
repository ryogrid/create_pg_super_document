# _h_spoolinit

## Location
[src/backend/access/hash/hashsort.c:60-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsort.c#L60-L98)

## Overview
Creates and initializes a hash index spool structure used during hash index construction to manage sorting and spooling of index tuples.

## Definition

```c
HSpool *
_h_spoolinit(Relation heap, Relation index, uint32 num_buckets)
```
## Detailed Description
This function allocates and initializes an HSpool structure that serves as the central data structure for managing hash index construction. The function calculates appropriate hash masks based on the number of buckets, which must be synchronized with the hash mask calculation in . It creates a tuplesort state using  (rather than ) to optimize index creation performance, as only one backend can perform index creation at a time.

The hash masks are computed using power-of-2 arithmetic where  represents the upper bound for hash values and  is half of that value, following PostgreSQL's hash bucket addressing scheme.

## Parameters / Member Variables
- `heap`: The heap relation being indexed
- `index`: The hash index relation being built
- `num_buckets`: The number of hash buckets in the index
## Dependencies
- Functions called/Symbols referenced:
  - [HSpool](../H/HSpool.md) (structure type)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (calculates next power of 2)
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md) (initializes tuple sorting state)
  - TUPLESORT_NONE (tuplesort option constant)
- Called from (representative examples):
  - [hashbuild](hashbuild.md)

## Notes and Other Information
- Uses maintenance_work_mem instead of work_mem to speed up index creation
- Hash mask calculation must remain synchronized with _hash_init_metabuffer
- The max_buckets field is set to num_buckets - 1 for zero-based indexing
- Memory is allocated using palloc0 to ensure zero-initialization of the structure

## Simplified Source

```c
HSpool *
_h_spoolinit(Relation heap, Relation index, uint32 num_buckets)
{
    HSpool *hspool = (HSpool *) palloc0(sizeof(HSpool));

    hspool->index = index;

    // Calculate hash masks for bucket addressing
    // Must stay synchronized with _hash_init_metabuffer
    hspool->high_mask = pg_nextpower2_32(num_buckets + 1) - 1;
    hspool->low_mask = (hspool->high_mask >> 1);
    hspool->max_buckets = num_buckets - 1;

    // Initialize tuple sorting for hash index construction
    // Use maintenance_work_mem for better index creation performance
    hspool->sortstate = tuplesort_begin_index_hash(heap,
                                                   index,
                                                   hspool->high_mask,
                                                   hspool->low_mask,
                                                   hspool->max_buckets,
                                                   maintenance_work_mem,
                                                   NULL,
                                                   TUPLESORT_NONE);

    return hspool;
}
```