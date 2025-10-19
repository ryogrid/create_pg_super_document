# box_contain_box

## Location
[src/backend/utils/adt/geo_ops.c:704-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L704-L721)

## Overview
Checks whether the second box is contained within the first box or lies on its border.

## Definition

```c
static bool
box_contain_box(BOX *contains_box, BOX *contained_box)
```
## Detailed Description
This function determines if one box (contained_box) is completely contained within another box (contains_box) or touches its border. The containment check is performed by comparing the coordinates of both boxes using floating-point comparison functions. A box is considered contained if all its boundary coordinates fall within or on the boundary of the containing box.

The function uses the BOX data structure which represents a rectangular box with low and high coordinate points in 2D space.

## Parameters / Member Variables
- `*contains_box`: Pointer to the BOX that potentially contains the other box
- `*contained_box`: Pointer to the BOX that is being tested for containment
## Dependencies
- Functions called/Symbols referenced:
  - [FPge](../F/FPge.md) (floating-point greater than or equal comparison)
  - [FPle](../F/FPle.md) (floating-point less than or equal comparison)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - [box_contained](box_contained.md)
  - [box_contain](box_contain.md)
  - [poly_contain_poly](../p/poly_contain_poly.md)

## Notes and Other Information
- This is a static function, only accessible within the geo_ops.c file
- Uses floating-point comparison functions (FPge, FPle) to handle potential precision issues
- Returns true if the contained_box is completely within or on the border of contains_box
- The containment logic checks both X and Y dimensions independently

## Simplified Source

```c
static bool box_contain_box(BOX *contains_box, BOX *contained_box) {
    // Check if contained_box fits entirely within contains_box
    // All boundaries of contained_box must be within or on contains_box boundaries

    bool x_contained = (contains_box->high.x >= contained_box->high.x) &&
                       (contains_box->low.x <= contained_box->low.x);

    bool y_contained = (contains_box->high.y >= contained_box->high.y) &&
                       (contains_box->low.y <= contained_box->low.y);

    return x_contained && y_contained;
}
```