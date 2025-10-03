# box_wd

## Location
[src/backend/utils/adt/geo_ops.c:883-892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L883-L892)

## Overview
Returns the width (horizontal magnitude) of a BOX geometric type by calculating the difference between the high and low x-coordinates.

## Definition

```c
static float8
box_wd(BOX *box)
```
## Detailed Description
The  function is a static helper function in PostgreSQL's geometric operations module that calculates the width of a BOX object. It computes the horizontal distance by subtracting the low x-coordinate from the high x-coordinate using the  function. This is an internal utility function used by other box-related operations that need to determine the horizontal extent of a box.

## Parameters / Member Variables
- `*box`: Pointer to a BOX structure containing the geometric box data with high and low coordinate points
## Dependencies
- Functions called/Symbols referenced:
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction function)
  - [BOX](../B/BOX.md) (geometric box data type)
- Called from (representative examples):
  - [box_width](box_width.md) (public function to get box width)
  - [box_ar](box_ar.md) (box area calculation function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_ops.c file
- The function assumes the BOX structure is properly initialized with valid high and low coordinates
- Uses PostgreSQL's float8_mi function for proper floating-point arithmetic handling
- The width is calculated as high.x - low.x, which should always be positive for a properly formed box