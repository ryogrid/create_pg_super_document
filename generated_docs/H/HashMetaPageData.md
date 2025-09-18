# HashMetaPageData

## Location
[src/include/access/hash.h:244-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash.h#L244-L265)

## Overview
HashMetaPageData is the structure that stores metadata information for a hash index, including table statistics, bucket management data, and overflow page allocation information.

## Definition


## Detailed Description
HashMetaPageData serves as the central control structure for hash indexes, containing all the essential metadata needed to manage the dynamic hash table structure. This includes information about the current size and organization of the table, overflow page management, and the mapping functions used for bucket allocation.

The structure supports PostgreSQL's dynamic hash table implementation, which can grow and shrink as needed while maintaining efficient access patterns. The splitpoint mechanism allows for controlled growth of the hash table, while the bitmap arrays manage overflow page allocation efficiently.

## Parameters / Member Variables
- : Magic number for hash table identification and validation
- : Version identifier for the hash index format
- : Current count of tuples stored in the entire hash table
- : Target fill factor (average tuples per bucket) used for split decisions
- : Size of index pages in bytes
- : Size of bitmap arrays in bytes (must be a power of 2)
- : Log base 2 of bitmap array size in bits (used for efficient bit operations)
- : Identifier of the highest numbered bucket currently in use
- : Bit mask used for modulo operations into the entire table
- : Bit mask used for modulo operations into the lower half of the table
- : Splitpoint from which overflow pages are being allocated
- : Bit number of the lowest numbered free overflow page
- : Total number of bitmap pages currently allocated
- : Procedure identifier for the hash function from pg_proc system catalog
- : Array tracking spare pages before each splitpoint
- : Array of block numbers for overflow bitmap pages

## Dependencies
- Functions called/Symbols referenced:
  - RegProcedure
  - HASH_MAX_SPLITPOINTS
  - HASH_MAX_BITMAPS
  - BlockNumber
- Called from (representative examples):
  - [_hash_init_metabuffer](../h/_hash_init_metabuffer.md)
  - [_hash_getcachedmetap](../h/_hash_getcachedmetap.md)
  - HashMetaPage

## Notes and Other Information
The dual mask system (highmask/lowmask) is a key optimization in the dynamic hash table implementation, allowing efficient bucket selection during table splits. The splitpoint mechanism enables controlled growth where the table can double in size incrementally rather than all at once. The bitmap management system provides efficient tracking of overflow page allocation, which is crucial for handling hash collisions and maintaining performance as the table grows.