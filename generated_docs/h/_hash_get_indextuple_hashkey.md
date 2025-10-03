# _hash_get_indextuple_hashkey

## Location
[src/backend/access/hash/hashutil.c:291-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L291-L317)

## Overview
Extracts the hash key value from a hash index tuple, providing fast access to the stored hash value.

## Definition
```c
uint32 _hash_get_indextuple_hashkey(IndexTuple itup)
```

## Detailed Description
This function efficiently retrieves the hash key value that is stored within a hash index tuple. Hash indexes store the computed hash value (rather than the original data) as the first attribute of each index tuple. The function uses a highly optimized approach that directly accesses the tuple's data area without going through the normal attribute access mechanisms.

The implementation makes several key assumptions for performance:
- The hash key is always the first attribute in the tuple
- The hash key can never be null
- The hash key is always a 32-bit unsigned integer

This "crude but very very cheaply" approach (as noted in the source comment) bypasses normal tuple processing overhead, making it suitable for high-frequency operations like searches, splits, and bucket management.

## Parameters
- `itup`: Pointer to the IndexTuple from which to extract the hash key

## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfoFindDataOffset](../I/IndexInfoFindDataOffset.md)
- Called from (representative examples):
  - [hashbucketcleanup](hashbucketcleanup.md)
  - [_hash_doinsert](_hash_doinsert.md)
  - [_hash_pgaddtup](_hash_pgaddtup.md)
  - [_hash_splitbucket](_hash_splitbucket.md)
  - [_hash_load_qualified_items](_hash_load_qualified_items.md)
  - [_h_indexbuild](_h_indexbuild.md)

## Simplified Source
```c
uint32 _hash_get_indextuple_hashkey(IndexTuple itup) {
    // Calculate pointer to tuple data (skip header)
    char *data_ptr = (char *) itup + IndexInfoFindDataOffset(itup->t_info);

    // Hash key is first attribute, cast to uint32 and return
    return *((uint32 *) data_ptr);
}
```

## Notes and Other Information
This function is fundamental to hash index operations and is called frequently during searches, insertions, deletions, and maintenance operations. Its optimized implementation reflects the critical performance requirements of hash index operations. The function assumes the standard hash index tuple format where the hash key is stored as the first 4 bytes of the tuple data area.