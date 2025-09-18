# contain4D

## Location
src/backend/utils/adt/geo_spgist.c: 260 - 267

## Overview
The `contain4D` function determines whether any rectangle from a rectangle box can completely contain a query argument in 4D space by performing 2D containment tests on both X and Y dimensions.

## Definition
```c
static bool contain4D(RectBox *rect_box, RangeBox *query)
```

## Detailed Description
This static helper function extends the 2D containment test to 4D space for rectangular geometric objects. The function performs containment detection by calling `contain2D` twice: once for the X-dimension range and once for the Y-dimension range. It returns true only if both dimensions show containment, implementing the logical AND condition for 4D containment detection in PostgreSQL spatial indexing. This ensures that the rectangle box fully encompasses the query range box in both spatial dimensions.

## Parameters / Member Variables
- `rect_box`: Pointer to a RectBox structure containing range boxes for X and Y dimensions that serves as the potential container
- `query`: Pointer to a RangeBox structure representing the query bounds to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - [contain2D](contain2D.md) (called twice for X and Y dimensions)
  - RectBox (structure type)
  - RangeBox (structure type)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This function is part of the SP-GiST implementation for geometric data types, specifically for rectangle containment operations
- Builds upon the 2D containment function to provide 4D (2D rectangle) containment testing
- Uses logical AND to ensure containment exists in both X and Y dimensions
- Primarily used in spatial index consistency checking during query processing for containment queries
- The function accesses range_box_x and range_box_y members from the RectBox, and left/right members from the query RangeBox
- Critical for implementing spatial containment operators in PostgreSQL geometric types