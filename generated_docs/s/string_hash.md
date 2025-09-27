# string_hash

## Location
[src/common/hashfn.c:660-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hashfn.c#L660-L676)

## Overview
The `string_hash` function is the default hash function for NUL-terminated string keys in PostgreSQL's hash table infrastructure, providing automatic length handling and truncation safety.

## Definition
```c
uint32 string_hash(const void *key, Size keysize)
```

## Detailed Description
The `string_hash` function serves as the standard hash function for string-based keys in PostgreSQL's hash table system. It provides a safe, convenient interface for hashing NUL-terminated strings while respecting hash table key size limitations. The function automatically determines the string length using `strlen()` and ensures that only the portion of the string that would fit in the hash table (up to `keysize-1` bytes) is actually hashed.

This truncation behavior is essential for hash table consistency - when strings are stored in hash tables, they may be truncated to fit the allocated key space, so the hash function must produce the same hash value for the string as it would exist in the table. The function delegates the actual hashing work to `hash_bytes()`, providing the string safety layer on top of the core hash algorithm.

As the default hash function, `string_hash` is automatically used when no specific hash function is provided during hash table creation, making it the foundation for most string-based hash tables in PostgreSQL.

## Parameters / Member Variables
- `key`: Pointer to the NUL-terminated string to be hashed (cast as void* for interface compatibility)
- `keysize`: Maximum size allocated for keys in the hash table (including space for NUL terminator)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_bytes](../h/hash_bytes.md) (core hashing algorithm)
- Called from (representative examples):
  - [dshash_strhash](../d/dshash_strhash.md) (src/backend/lib/dshash.c:615)
  - [hash_create](../h/hash_create.md) (src/backend/utils/hash/dynahash.c:434,448,458)
  - [hash_uint32_extended](../h/hash_uint32_extended.md) (src/include/common/hashfn.h:55)

## Notes and Other Information
- Serves as the default hash function for PostgreSQL's hash table infrastructure when no specific function is specified
- Automatically handles string length calculation and ensures hash consistency with table key truncation behavior
- Prevents hash inconsistencies that could occur if the full string were hashed but only a truncated portion stored
- Essential for maintaining hash table integrity when dealing with variable-length string keys
- Acts as a safe wrapper around hash_bytes for string-specific use cases
- The keysize parameter includes space for the NUL terminator, so effective string length is keysize-1

## Simplified Source

```c
// Simplified version of string_hash
uint32 string_hash(const void *key, Size keysize) {
    // Step 1: Calculate the actual string length
    Size string_length = strlen((const char *) key);

    // Step 2: Ensure we don't exceed hash table key capacity
    // (keysize-1 because keysize includes space for NUL terminator)
    Size hash_length = Min(string_length, keysize - 1);

    // Step 3: Hash only the portion that would fit in the hash table
    return hash_bytes((const unsigned char *) key, (int) hash_length);
}
```

Key simplifications made:
- Added descriptive variable names (string_length, hash_length)
- Broke down the logic into clear sequential steps with comments
- Emphasized the purpose of each operation
- Maintained the exact same functionality and algorithm