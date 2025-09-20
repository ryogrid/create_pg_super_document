# box_ht

## Location
[src/backend/utils/adt/geo_ops.c:893-907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L893-L907)

## Overview
Returns the height (vertical magnitude) of a BOX geometric type by calculating the difference between the high and low y-coordinates.

## Definition

```c
static float8
box_ht(BOX *box)
```
## Detailed Description
The  function is a static helper function in PostgreSQL's geometric operations module that calculates the height of a BOX object. It computes the vertical distance by subtracting the low y-coordinate from the high y-coordinate using the  function. This is an internal utility function used by other box-related operations that need to determine the vertical extent of a box.

## Parameters / Member Variables
- : Pointer to a BOX structure containing the geometric box data with high and low coordinate points

## Dependencies
- Functions called/Symbols referenced:
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction function)
  - [BOX](../B/BOX.md) (geometric box data type)
- Called from (representative examples):
  - [box_height](box_height.md) (public function to get box height)
  - [box_ar](box_ar.md) (box area calculation function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_ops.c file
- The function assumes the BOX structure is properly initialized with valid high and low coordinates
- Uses PostgreSQL's float8_mi function for proper floating-point arithmetic handling
- The height is calculated as high.y - low.y, which should always be positive for a properly formed box
- Complements the box_wd function for complete dimensional analysis of boxes