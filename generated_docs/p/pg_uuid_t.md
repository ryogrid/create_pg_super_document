# pg_uuid_t

## Location
[src/include/utils/uuid.h:20-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/uuid.h#L20-L23)

## Overview
A structure that represents a Universally Unique Identifier (UUID) within PostgreSQL, storing 128-bit UUID data as a byte array.

## Definition
```c
typedef struct pg_uuid_t
{
    unsigned char data[UUID_LEN];
} pg_uuid_t;
```
where UUID_LEN is defined as 16 bytes.

## Detailed Description
pg_uuid_t is PostgreSQL's internal representation of UUID values. The structure contains a single member - a 16-byte array that stores the raw UUID data in binary format. This representation follows the standard UUID format as defined in RFC 4122, where UUIDs are 128-bit (16-byte) values.

The structure provides a type-safe way to handle UUID data throughout PostgreSQL's codebase, ensuring that UUID operations maintain proper type checking and semantic clarity. All UUID-related functions in PostgreSQL operate on this structure type.

## Parameters / Member Variables
- `data[UUID_LEN]`: A 16-byte unsigned char array that stores the raw binary representation of the UUID. The array contains the UUID in network byte order (big-endian) format.

## Dependencies
- Functions called/Symbols referenced:
  - UUID_LEN (constant defining 16 bytes)
- Called from (representative examples):
  - [brin_minmax_multi_distance_uuid](../b/brin_minmax_multi_distance_uuid.md)
  - [uuid_in](../u/uuid_in.md), uuid_out, uuid_recv, uuid_send
  - [uuid_lt](../u/uuid_lt.md), uuid_le, uuid_eq, uuid_ge, uuid_gt, uuid_ne
  - [uuid_cmp](../u/uuid_cmp.md), uuid_fast_cmp
  - [uuid_hash](../u/uuid_hash.md), uuid_hash_extended
  - [gen_random_uuid](../g/gen_random_uuid.md)
  - uuid_extract_timestamp, uuid_extract_version
  - [UUIDPGetDatum](../U/UUIDPGetDatum.md), DatumGetUUIDP

## Notes and Other Information
- The 16-byte size accommodates the standard 128-bit UUID format
- Used extensively throughout PostgreSQL's UUID implementation for input/output, comparison, hashing, and indexing operations
- The structure is designed to be efficiently passed by pointer in most operations
- Binary format allows for efficient storage and fast comparison operations
- Supports all UUID versions (1, 3, 4, 5) as defined in RFC 4122