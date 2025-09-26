# PageGetContents

## Location
[src/include/storage/bufpage.h:255-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L255-L273)

## Overview
PageGetContents is an inline function that returns a pointer to the content area of a page, specifically designed for pages that do not contain line pointers and store data directly after the page header.

## Definition

```c
static inline char *
PageGetContents(Page page)
```
## Detailed Description
This function calculates and returns a pointer to the content area of a page by adding the maximum-aligned size of the page header to the page base address. It is specifically intended for use with pages that do not use line pointers (such as meta pages, bitmap pages, and other special-purpose pages). The function ensures proper memory alignment by using MAXALIGN on the page header size, which is critical for performance and correctness on architectures that require aligned access. Prior to PostgreSQL 8.3, this function did not guarantee proper alignment, but it now ensures that the returned pointer is properly aligned for any data type.

## Parameters / Member Variables
- : A pointer to a page (Page type) for which to get the content area

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfPageHeaderData (macro defining size of page header without line pointers)
  - MAXALIGN (macro for ensuring maximum alignment of memory addresses)
- Called from (representative examples):
  - [brinGetStats](../b/brinGetStats.md) (in src/backend/access/brin/brin.c:1648)
  - [visibilitymap_clear](../v/visibilitymap_clear.md) (in src/backend/access/heap/visibilitymap.c:159)
  - [fsm_set_avail](../f/fsm_set_avail.md) (in src/backend/storage/freespace/fsmpage.c:66)
  - GinPageGetMeta (in src/include/access/ginblock.h:106)
  - HashPageGetMeta (in src/include/access/hash.h:324)
  - BTPageGetMeta (in src/include/access/nbtree.h:122)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- Designed specifically for pages without line pointers (meta pages, bitmap pages, etc.)
- Ensures proper memory alignment using MAXALIGN, which is critical for performance
- Returns a char* pointer that can be cast to the appropriate data structure type
- Should NOT be used for regular heap pages or index pages that contain tuples with line pointers
- The alignment guarantee was added in PostgreSQL 8.3 - older code may incorrectly assume simple SizeOfPageHeaderData offset
- Widely used across access methods for accessing metadata stored directly on pages
- Used for accessing data structures like meta pages, free space maps, and visibility maps
- The returned pointer points to immediately after the properly aligned page header