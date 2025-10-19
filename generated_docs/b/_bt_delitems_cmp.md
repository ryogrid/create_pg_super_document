# _bt_delitems_cmp

## Location
[src/backend/access/nbtree/nbtpage.c:1464-1512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1464-L1512)

## Overview
A comparator function used to restore the deltids array back to its original leaf-page-wise sort order by comparing TM_IndexDelete structure IDs.

## Definition

```c
static int
_bt_delitems_cmp(const void *a, const void *b)
```
## Detailed Description
This is a simple comparison function designed for use with sorting algorithms (such as ) to order  structures by their ID field. The function is specifically used by  to restore a deltids array back to its original leaf-page-wise sort order.

The function follows the standard C library comparator interface, returning:
- A negative value if the first element should come before the second
- Zero if the elements are equal (though the assertion indicates this should never happen)
- A positive value if the first element should come after the second

The comparison is performed using  which safely compares 16-bit signed integers, handling the ID field of the TM_IndexDelete structures.

## Parameters / Member Variables
- `*a`: Pointer to the first TM_IndexDelete structure to compare
- `*b`: Pointer to the second TM_IndexDelete structure to compare
## Dependencies
- Functions called/Symbols referenced:
  - : Structure type containing index deletion information
  - : PostgreSQL utility function for comparing 16-bit signed integers
- Called from:
  - : Used to sort deltids array for page-wise ordering

## Notes and Other Information
- The function includes an assertion that the IDs being compared are never equal, indicating that duplicate IDs should not occur in the context where this comparator is used
- This is a static function, meaning it's only accessible within the same source file (nbtpage.c)
- The function is designed to work with the standard C library sorting functions that expect comparators with this signature
- The "leaf-page-wise sort order" refers to ordering items by their position/ID within a B-tree leaf page structure

## Simplified Source

```c
static int _bt_delitems_cmp(const void *a, const void *b) {
    // Cast void pointers to TM_IndexDelete structures
    TM_IndexDelete *indexdelete1 = (TM_IndexDelete *) a;
    TM_IndexDelete *indexdelete2 = (TM_IndexDelete *) b;

    // IDs should never be equal in valid usage
    Assert(indexdelete1->id != indexdelete2->id);

    // Compare the IDs using PostgreSQL's 16-bit signed integer comparator
    return pg_cmp_s16(indexdelete1->id, indexdelete2->id);
}
```