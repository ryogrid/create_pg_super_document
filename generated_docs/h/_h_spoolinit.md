# _h_spoolinit

## Location
src/backend/access/hash/hashsort.c: 60 - 98

## Overview
Creates and initializes a hash index spool structure used during hash index construction to manage sorting and spooling of index tuples.

## Definition


## Detailed Description
This function allocates and initializes an HSpool structure that serves as the central data structure for managing hash index construction. The function calculates appropriate hash masks based on the number of buckets, which must be synchronized with the hash mask calculation in . It creates a tuplesort state using  (rather than ) to optimize index creation performance, as only one backend can perform index creation at a time.

The hash masks are computed using power-of-2 arithmetic where  represents the upper bound for hash values and  is half of that value, following PostgreSQL's hash bucket addressing scheme.

## Parameters / Member Variables
- : The heap relation being indexed
- : The hash index relation being built
- : The number of hash buckets in the index

## Dependencies
- Functions called/Symbols referenced:
  - [HSpool](../H/HSpool.md) (structure type)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (calculates next power of 2)
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md) (initializes tuple sorting state)
  - TUPLESORT_NONE (tuplesort option constant)
- Called from (representative examples):
  - [hashbuild](hashbuild.md)

## Notes and Other Information
- Uses  instead of  to speed up index creation
- Hash mask calculation must remain synchronized with 
- The  field is set to  for zero-based indexing
- Memory is allocated using  to ensure zero-initialization of the structure