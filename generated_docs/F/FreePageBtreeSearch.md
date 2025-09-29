# FreePageBtreeSearch

## Location
[src/backend/utils/mmgr/freepage.c:1064-1139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1064-L1139)

## Overview
Searches the btree for an entry with the given first page and initializes the search result structure with position information for exact matches or insertion points.

## Definition
```c
static void FreePageBtreeSearch(FreePageManager *fpm, Size first_page, FreePageBtreeSearchResult *result)
```

## Detailed Description
This function performs a comprehensive search through the FreePageBtree structure to locate a specific page entry or determine where a new entry should be inserted. The search process begins at the root and descends through internal nodes until reaching a leaf page. During traversal, it also calculates the number of additional btree pages that would be needed for potential splits during insertion operations.

The function handles both exact matches and insertion scenarios. For exact matches, it returns the precise location of the entry. For insertion cases, it identifies the correct position where the new key should be placed while maintaining the btree ordering properties.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager structure containing the btree root and segment information
- `first_page`: The target page number to search for in the btree
- `result`: Output parameter that will contain the search results including page location, index, match status, and split requirements

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - relptr_access
  - [FreePageBtreeSearchInternal](FreePageBtreeSearchInternal.md)
  - [FreePageBtreeSearchLeaf](FreePageBtreeSearchLeaf.md)
- Called from:
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
- The function calculates split_pages to indicate how many additional btree pages would be needed for insertion
- Uses magic numbers (FREE_PAGE_INTERNAL_MAGIC) to distinguish between internal and leaf pages during traversal
- Implements btree descent logic by choosing appropriate child nodes based on key comparisons
- Maintains btree invariants by ensuring proper parent-child relationships during traversal
- The search result structure provides complete information needed for both lookup and insertion operations

## Simplified Source

```c
static void
FreePageBtreeSearch(FreePageManager *fpm, Size first_page,
                    FreePageBtreeSearchResult *result)
{
    char *base = fpm_segment_base(fpm);
    FreePageBtree *btp = relptr_access(base, fpm->btree_root);
    Size index;

    result->split_pages = 1;

    // Empty btree case
    if (btp == NULL)
    {
        result->page = NULL;
        result->found = false;
        return;
    }

    // Descend through internal nodes to leaf
    while (btp->hdr.magic == FREE_PAGE_INTERNAL_MAGIC)
    {
        index = FreePageBtreeSearchInternal(btp, first_page);
        bool found_exact = index < btp->hdr.nused &&
            btp->u.internal_key[index].first_page == first_page;

        // For non-exact matches, go to left child for insertion point
        if (!found_exact && index > 0)
            --index;

        // Track pages needed for potential splits
        if (btp->hdr.nused >= FPM_ITEMS_PER_INTERNAL_PAGE)
        {
            Assert(btp->hdr.nused == FPM_ITEMS_PER_INTERNAL_PAGE);
            result->split_pages++;
        }
        else
            result->split_pages = 0;

        // Move to child page
        Assert(index < btp->hdr.nused);
        FreePageBtree *child = relptr_access(base, btp->u.internal_key[index].child);
        Assert(relptr_access(base, child->hdr.parent) == btp);
        btp = child;
    }

    // Check if leaf page would need splitting
    if (btp->hdr.nused >= FPM_ITEMS_PER_LEAF_PAGE)
    {
        Assert(btp->hdr.nused == FPM_ITEMS_PER_INTERNAL_PAGE);
        result->split_pages++;
    }
    else
        result->split_pages = 0;

    // Search the leaf page
    index = FreePageBtreeSearchLeaf(btp, first_page);

    // Set up results
    result->page = btp;
    result->index = index;
    result->found = index < btp->hdr.nused &&
        first_page == btp->u.leaf_key[index].first_page;
}
```