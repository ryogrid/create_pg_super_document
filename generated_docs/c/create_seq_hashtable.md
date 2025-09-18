# create_seq_hashtable

## Location
src/backend/commands/sequence.c: 1113 - 1128

## Overview
Creates and initializes the hash table used for storing sequence data in the backend.

## Definition
```c
static void create_seq_hashtable(void)
```

## Detailed Description
This static function initializes the global sequence hash table (`seqhashtab`) that PostgreSQL uses to cache sequence information during a session. The hash table maps sequence relation OIDs to their corresponding SeqTableData structures, providing efficient lookup and storage of sequence metadata.

The function sets up a hash table with specific parameters optimized for sequence operations: it uses Oid values as keys and SeqTableData structures as entries. The hash table is created with an initial size of 16 entries and uses the HASH_ELEM and HASH_BLOBS flags to indicate that it stores fixed-size elements and should use simple byte-wise comparison for keys.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - HASHCTL (hash table control structure)
  - [SeqTableData](../S/SeqTableData.md) (sequence table data structure)
  - [hash_create](../h/hash_create.md) (creates a new hash table)
  - HASH_ELEM (hash table flag for fixed-size elements)
  - HASH_BLOBS (hash table flag for byte-wise key comparison)
- Called from (representative examples):
  - [init_sequence](../i/init_sequence.md)

## Notes and Other Information
- The hash table is named "Sequence values" for debugging and monitoring purposes
- Initial size is set to 16 entries, which will grow as needed
- HASH_BLOBS flag is used because Oid keys can be compared byte-wise
- This is a static function internal to src/backend/commands/sequence.c
- The created hash table is stored in the global `seqhashtab` variable