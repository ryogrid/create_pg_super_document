# dshash_table_item

## Location
[src/backend/lib/dshash.c:44-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L44-L58)

## Overview
The dshash_table_item struct represents an item in a dynamic shared hash table, wrapping the user's entry object in an envelope that maintains pointers for hash table management.

## Definition


## Detailed Description
The dshash_table_item struct serves as a wrapper around user-defined data entries in PostgreSQL's dynamic shared hash table implementation. This structure provides the necessary metadata for hash table operations while maintaining a separation between the hash table's internal management data and the user's actual data. The struct implements a linked list structure for collision resolution within hash buckets, and caches the computed hash value to avoid recomputation during operations like resizing.

## Parameters / Member Variables
- : A dsa_pointer that points to the next item in the same hash bucket, implementing collision resolution through chaining
- hash: hash table empty: A cached dshash_hash value containing the precomputed hash of the key, used to avoid recalculating the hash during table operations
- User data follows immediately after this struct in memory (accessed via ENTRY_FROM_ITEM macro)

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
  - dshash_hash
- Called from (representative examples):
  - ENTRY_FROM_ITEM (macro for accessing user data)
  - ITEM_FROM_ENTRY (macro for getting item from user data)
  - find_in_bucket
  - [insert_item_into_bucket](../i/insert_item_into_bucket.md)
  - [delete_item_from_bucket](delete_item_from_bucket.md)
  - [dshash_find](dshash_find.md)
  - [dshash_find_or_insert](dshash_find_or_insert.md)
  - [dshash_delete_entry](dshash_delete_entry.md)

## Notes and Other Information
- The user's actual data follows immediately after this struct in memory, creating a single allocation that contains both the hash table metadata and the user data
- The hash value is cached to optimize performance during table resizing operations where items need to be redistributed
- This struct is part of PostgreSQL's dynamic shared area (DSA) infrastructure, allowing hash tables to be shared across multiple processes
- The next pointer enables collision resolution through separate chaining within hash buckets