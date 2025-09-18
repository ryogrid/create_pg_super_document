# GISTENTRY

## Location
[src/include/access/gist.h:158-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist.h#L158-L165)

## Overview
GISTENTRY represents an entry on a GiST index node, containing the key data along with its physical location information and metadata about whether it's in a leaf node.

## Definition
```c
typedef struct GISTENTRY
{
    Datum        key;
    Relation     rel;
    Page         page;
    OffsetNumber offset;
    bool         leafkey;
} GISTENTRY;
```

## Detailed Description
GISTENTRY serves as the fundamental data structure for representing individual entries within GiST index nodes. Each entry contains both the actual key data and sufficient location metadata to identify where the entry physically resides within the index structure.

This structure is used extensively throughout GiST operations including searches, insertions, deletions, and page splits. The structure allows GiST methods to access both the logical key content and the physical storage details, enabling operations that need to navigate or modify the index structure.

The leafkey flag is particularly important for GiST algorithms, as leaf and internal node entries often require different processing strategies. Leaf entries point to actual table tuples, while internal node entries point to child index pages and contain union keys that encompass all keys in their subtrees.

## Parameters / Member Variables
- `key`: Datum - The actual key data for this entry, which may be compressed or in a specialized format depending on the data type
- `rel`: Relation - Pointer to the relation (table/index) structure that contains this entry
- `page`: Page - The specific page within the relation where this entry is stored
- `offset`: OffsetNumber - The offset number of this entry within its page, used for precise tuple identification
- `leafkey`: bool - Flag indicating whether this entry belongs to a leaf node (true) or internal node (false)

## Dependencies
- Functions called/Symbols referenced:
  - Datum
  - [Relation](../R/Relation.md)  
  - Page
  - OffsetNumber
- Called from (representative examples):
  - [gistindex_keytest](../g/gistindex_keytest.md)
  - [gist_box_consistent](../g/gist_box_consistent.md)
  - [gist_box_penalty](../g/gist_box_penalty.md)
  - [gist_poly_compress](../g/gist_poly_compress.md)
  - [gistMakeUnionKey](../g/gistMakeUnionKey.md)
  - gistdentryinit
  - [gistCompressValues](../g/gistCompressValues.md)
  - [inet_gist_consistent](../i/inet_gist_consistent.md)
  - [range_gist_consistent](../r/range_gist_consistent.md)
  - [gtsvector_compress](../g/gtsvector_compress.md)

## Notes and Other Information
- [GISTENTRY](GISTENTRY.md) instances are commonly created and manipulated during index traversal and maintenance operations
- The structure provides the bridge between logical key operations (handled by data type-specific methods) and physical storage management (handled by GiST core code)
- Different GiST operator classes use the same GISTENTRY structure but interpret the key field according to their specific data type requirements
- The combination of rel, page, and offset provides a complete physical address for the entry, supporting operations that need to modify or reference the stored data
- Many GiST support functions receive GISTENTRY pointers as parameters, allowing them to access both key data and storage context