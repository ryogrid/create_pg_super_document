# json_unique_hash

## Location
[src/backend/utils/adt/json.c:891-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L891-L901)

## Overview
The  function computes hash values for JSON object keys to enable fast duplicate key detection in JSON aggregation operations through hash table lookups.

## Definition

```c
static uint32
json_unique_hash(const void *key, Size keysize)
```
## Detailed Description
This function serves as a hash function for the hash table used in JSON key uniqueness checking. It takes a  structure as input and computes a 32-bit hash value by combining two hash components: the object ID (using ) and the key string (using ). The hash combination uses XOR to blend the two hash values, providing good distribution for the hash table.

The function is designed to be used as a callback function for PostgreSQL's hash table implementation (HTAB), enabling efficient detection of duplicate keys within JSON objects during aggregation operations.

## Parameters / Member Variables
- `*key`: Pointer to a  structure containing the key data to hash
- `keysize`: Size parameter (required by hash table interface but not directly used)
## Dependencies
- Functions called/Symbols referenced:
  - : Structure containing key string, key length, and object ID
  - : Optimized hash function for the 32-bit object ID
  - : Core hash function for the variable-length key string
  - : Converts the computed hash to PostgreSQL's Datum format

- Called from (representative examples):
  - : Used as hash function callback when creating the hash table

## Notes and Other Information
- This is a static function internal to the JSON aggregate implementation
- The hash computation combines both the object ID and the key string to ensure proper separation of keys across different JSON objects in nested structures
- Uses XOR operation to combine the two hash components, which provides good hash distribution
- The function signature follows PostgreSQL's hash table callback function interface
- Part of the fast key uniqueness checking system that prevents duplicate keys in JSON objects during aggregation
- The hash quality is important for performance, as poor distribution would lead to hash collisions and slower duplicate detection

## Simplified Source

```c
static uint32
json_unique_hash(const void *key, Size keysize)
{
    const JsonUniqueHashEntry *entry = (JsonUniqueHashEntry *) key;

    // Hash the object ID
    uint32 hash = hash_bytes_uint32(entry->object_id);

    // Combine with hash of the key string using XOR
    hash ^= hash_bytes((const unsigned char *) entry->key, entry->key_len);

    return DatumGetUInt32(hash);
}
```