# list_oid_cmp

## Location
[src/backend/nodes/list.c:1703-1709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1703-L1709)

## Overview
A comparator function used by list_sort to sort PostgreSQL lists containing OID (Object Identifier) values in ascending order.

## Definition

```c
int
list_oid_cmp(const ListCell *p1, const ListCell *p2)
```
## Detailed Description
The  function serves as a comparison function specifically designed for use with PostgreSQL's  function when sorting lists that contain OID (Object Identifier) values. It extracts OID values from two list cells and compares them using PostgreSQL's standard 32-bit unsigned integer comparison function. OIDs are fundamental identifiers in PostgreSQL used to uniquely identify database objects such as tables, functions, and types. This function follows the standard C library comparator convention, returning a negative value if the first element is smaller, zero if they are equal, and a positive value if the first element is larger.

## Parameters / Member Variables
- : Pointer to the first ListCell containing an OID value to be compared
- : Pointer to the second ListCell containing an OID value to be compared

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts OID value from ListCell
  -  - PostgreSQL's 32-bit unsigned integer comparison function
- Called from (representative examples):
  -  (src/backend/catalog/heap.c:3365)
  -  (src/backend/catalog/pg_publication.c:749)
  -  (src/backend/utils/cache/relcache.c:4882)
  -  (src/backend/utils/cache/relcache.c:4974)
  - Used in  macro context (src/include/nodes/pg_list.h:684)

## Notes and Other Information
- This function is specifically designed for sorting lists containing OID values in ascending order
- OIDs are treated as unsigned 32-bit integers, hence the use of  instead of signed comparison
- It leverages PostgreSQL's portable comparison functions to ensure consistent behavior across different platforms
- The function is commonly used in catalog operations where lists of object identifiers need to be sorted
- Widely used throughout PostgreSQL's catalog system for maintaining sorted lists of database object identifiers
- Located in src/backend/nodes/list.c:1703-1709

## Simplified Source

```c
int list_oid_cmp(const ListCell *p1, const ListCell *p2) {
    // Extract OID values from list cells
    Oid v1 = lfirst_oid(p1);
    Oid v2 = lfirst_oid(p2);

    // Compare using 32-bit unsigned integer comparison
    return pg_cmp_u32(v1, v2);
}
```