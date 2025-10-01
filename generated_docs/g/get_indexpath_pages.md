# get_indexpath_pages

## Location
[src/backend/optimizer/path/costsize.c:963-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L963-L1012)

## Overview
Determines the total size (in pages) of all indexes used in a bitmap index path by recursively traversing the bitmap qualification tree.

## Definition
```c
static double get_indexpath_pages(Path *bitmapqual)
```

## Detailed Description
This function calculates the total number of pages across all indexes involved in a bitmap index scan path. It recursively traverses the bitmap qualification tree structure, handling three types of path nodes:

- **BitmapAndPath**: Sums up pages from all child bitmap qualifications in an AND operation
- **BitmapOrPath**: Sums up pages from all child bitmap qualifications in an OR operation  
- **IndexPath**: Returns the actual page count from the index metadata

The function uses a recursive approach to handle nested bitmap operations, ensuring all indexes in the qualification tree are accounted for. Note that if the same index appears multiple times in the bitmap tree, it will be counted multiple times, which may not be ideal but detecting duplicates is complex.

## Parameters / Member Variables
- `bitmapqual`: A Path pointer representing the root of a bitmap qualification tree, which can be a BitmapAndPath, BitmapOrPath, or IndexPath

## Dependencies
- Functions called/Symbols referenced:
  - [BitmapAndPath](../B/BitmapAndPath.md) (struct type)
  - [BitmapOrPath](../B/BitmapOrPath.md) (struct type) 
  - [IndexPath](../I/IndexPath.md) (struct type)
  - nodeTag (function)
  - [get_indexpath_pages](get_indexpath_pages.md) (recursive self-call)

- Called from:
  - [compute_bitmap_pages](../c/compute_bitmap_pages.md) (in costsize.c:6459)

## Notes and Other Information
- This is a static function internal to the cost estimation module
- The function handles nested bitmap operations through recursion
- Same indexes may be counted multiple times if they appear in different parts of the bitmap tree
- Error handling includes logging unrecognized node types
- Located in src/backend/optimizer/path/costsize.c:963-1012

## Simplified Source

```c
static double
get_indexpath_pages(Path *bitmapqual)
{
    double result = 0;

    if (IsA(bitmapqual, BitmapAndPath))
    {
        // Sum pages from all AND children
        BitmapAndPath *apath = (BitmapAndPath *) bitmapqual;

        foreach(ListCell *l, apath->bitmapquals)
        {
            result += get_indexpath_pages((Path *) lfirst(l));
        }
    }
    else if (IsA(bitmapqual, BitmapOrPath))
    {
        // Sum pages from all OR children
        BitmapOrPath *opath = (BitmapOrPath *) bitmapqual;

        foreach(ListCell *l, opath->bitmapquals)
        {
            result += get_indexpath_pages((Path *) lfirst(l));
        }
    }
    else if (IsA(bitmapqual, IndexPath))
    {
        // Get actual page count from index
        IndexPath *ipath = (IndexPath *) bitmapqual;
        result = (double) ipath->indexinfo->pages;
    }
    else
    {
        elog(ERROR, "unrecognized node type: %d", nodeTag(bitmapqual));
    }

    return result;
}
```