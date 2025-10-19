# adjustBox

## Location
[src/backend/access/gist/gistproc.c:146-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L146-L163)

## Overview
Expands a BOX to include another BOX by updating its coordinates to encompass the minimum bounding rectangle that contains both boxes.

## Definition
```c
static void adjustBox(BOX *b, const BOX *addon)
```

## Detailed Description
This function modifies an existing BOX structure in-place to ensure it encompasses both the original box and an additional "addon" box. It effectively computes the union of two boxes by adjusting the coordinates of the first box to include the second box's extent.

The function works by comparing each coordinate pair:
- For high coordinates (top-right corner): takes the maximum values to ensure the expanded box reaches the furthest extents
- For low coordinates (bottom-left corner): takes the minimum values to ensure the expanded box covers the nearest extents

This operation is fundamental in spatial indexing algorithms where bounding boxes need to be incrementally expanded to accommodate new entries or when merging spatial regions.

## Parameters / Member Variables
- `b`: Input/Output parameter - pointer to the BOX structure that will be modified to include the addon box
- `addon`: Input parameter - pointer to the BOX structure that should be included in the adjusted box (read-only)

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type)
  - [float8_lt](../f/float8_lt.md) (for comparing coordinates to determine if expansion is needed)
  - [float8_gt](../f/float8_gt.md) (for comparing coordinates to determine if expansion is needed)
- Called from (representative examples):
  - [gist_box_union](../g/gist_box_union.md)
  - [fallbackSplit](../f/fallbackSplit.md) (multiple times)
  - [gist_box_picksplit](../g/gist_box_picksplit.md)
  - PLACE_LEFT
  - PLACE_RIGHT

## Notes and Other Information
- This is a static function, only accessible within gistproc.c
- The function modifies the first box parameter in-place rather than creating a new box
- Unlike rt_box_union, this function modifies an existing box rather than creating a union in a separate output parameter
- The operation is not commutative in terms of which box gets modified - [adjustBox](adjustBox.md)(a, b) modifies 'a' to include 'b'
- Essential for maintaining bounding box integrity during R-tree node updates and splits
- Used extensively in GiST split algorithms and union operations
- Located in src/backend/access/gist/gistproc.c:146-163
- The coordinate comparisons ensure that the box only expands when necessary, maintaining efficiency
- Critical component of spatial index maintenance operations in PostgreSQL's GiST implementation

## Simplified Source

```c
static void
adjustBox(BOX *b, const BOX *addon)
{
    // Expand box b to include addon by taking max high and min low coordinates
    if (float8_lt(b->high.x, addon->high.x))
        b->high.x = addon->high.x;
    if (float8_gt(b->low.x, addon->low.x))
        b->low.x = addon->low.x;
    if (float8_lt(b->high.y, addon->high.y))
        b->high.y = addon->high.y;
    if (float8_gt(b->low.y, addon->low.y))
        b->low.y = addon->low.y;
}
```