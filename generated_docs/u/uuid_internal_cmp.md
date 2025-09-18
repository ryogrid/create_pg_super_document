# uuid_internal_cmp

## Location
[src/backend/utils/adt/uuid.c:168-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L168-L173)

## Overview
Internal comparison function that performs lexicographic comparison of two UUID values by comparing their raw byte data.

## Definition


## Detailed Description
This is a static internal helper function that provides the core comparison logic for all UUID comparison operations in PostgreSQL. It performs a byte-wise comparison of two UUID values using the standard  function, treating UUIDs as 16-byte binary values. The function returns an integer indicating the relative ordering of the two UUIDs: negative if arg1 < arg2, zero if equal, and positive if arg1 > arg2. This lexicographic comparison ensures consistent ordering behavior across all UUID operations.

## Parameters / Member Variables
- : Pointer to the first UUID value to compare
- : Pointer to the second UUID value to compare

## Dependencies
- Functions called/Symbols referenced:
  - memcmp (standard C library function)
  - [pg_uuid_t](../p/pg_uuid_t.md) (UUID data type structure)
  - UUID_LEN (constant defining UUID length as 16 bytes)
- Called from (representative examples):
  - [uuid_lt](uuid_lt.md)
  - [uuid_le](uuid_le.md) 
  - [uuid_eq](uuid_eq.md)
  - [uuid_ge](uuid_ge.md)
  - [uuid_gt](uuid_gt.md)
  - [uuid_ne](uuid_ne.md)
  - [uuid_cmp](uuid_cmp.md)
  - [uuid_fast_cmp](uuid_fast_cmp.md)

## Notes and Other Information
This function is marked as static and is only used internally within the uuid.c module. All public UUID comparison functions delegate to this internal implementation, ensuring consistent comparison semantics across all UUID operations. The lexicographic byte comparison means that UUID ordering is based purely on the binary representation, not on any semantic interpretation of UUID structure or version.