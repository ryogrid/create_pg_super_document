# oid_cmp

## Location
[src/backend/utils/adt/oid.c:258-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L258-L271)

## Overview
A comparison function for Oid (Object Identifier) values designed for use with qsort and other sorting algorithms that require a standard comparison callback.

## Definition

```c
int
oid_cmp(const void *p1, const void *p2)
```
## Detailed Description
The oid_cmp function provides a standardized comparison mechanism for Oid values, following the conventional comparison function signature required by qsort and similar sorting utilities. It takes two void pointers to Oid values, dereferences them, and delegates the actual comparison to pg_cmp_u32, which performs an unsigned 32-bit integer comparison. This function returns a negative value if the first Oid is less than the second, zero if they are equal, and a positive value if the first is greater than the second.

## Parameters / Member Variables
- `p1`: Pointer to the first Oid value to compare
- `p2`: Pointer to the second Oid value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u32](../p/pg_cmp_u32.md)
- Called from (representative examples):
  - [EnumValuesCreate](../E/EnumValuesCreate.md)
  - [find_inheritance_children_extended](../f/find_inheritance_children_extended.md)
  - [aclmembers](../a/aclmembers.md)

## Notes and Other Information
This function is specifically designed as a qsort comparison callback, hence the void pointer parameters and integer return type following the standard comparison function contract. The function is used in various parts of PostgreSQL where Oid arrays need to be sorted, such as in enum value creation, inheritance hierarchy processing, and ACL member handling.