# point_zorder_internal

## Location
src/backend/access/gist/gistproc.c: 1575 - 1585

## Overview
A static utility function that computes the Z-order value (Morton code) of a 2D point with floating-point coordinates for spatial indexing and fast index building in GiST.

## Definition
```c
static uint64 point_zorder_internal(float4 x, float4 y)
```

## Detailed Description
This function implements the Z-order curve (also known as Morton code) mapping algorithm that converts a two-dimensional point into a single 64-bit integer while preserving spatial locality. Points that are close in 2D space are mapped to integers that are numerically close to each other. The algorithm works by interleaving the bits of the X and Y coordinates after converting the IEEE 754 floating-point values to unsigned integers. This technique is particularly useful for spatial indexing operations where maintaining locality is crucial for performance, such as during GiST index construction.

## Parameters / Member Variables
- `x`: The X-coordinate as a 32-bit float (float4)
- `y`: The Y-coordinate as a 32-bit float (float4)

## Dependencies
- Functions called/Symbols referenced:
  - [ieee_float32_to_uint32](../i/ieee_float32_to_uint32.md) (converts IEEE 754 float to uint32)
  - [part_bits32_by2](part_bits32_by2.md) (spreads 32-bit value bits by factor of 2)
- Called from:
  - [gist_bbox_zorder_cmp](../g/gist_bbox_zorder_cmp.md) (multiple calls for bounding box corners)
  - [gist_bbox_zorder_abbrev_convert](../g/gist_bbox_zorder_abbrev_convert.md)

## Notes and Other Information
- The function assumes IEEE 754 floating-point format for input coordinates
- Uses bit interleaving technique: alternates bits from X and Y coordinates to create the final 64-bit Morton code
- The resulting Z-order value preserves spatial locality, making it suitable for spatial sorting and indexing
- Part of PostgreSQL's GiST fast index build infrastructure
- The algorithm maps 2D coordinates to a 1D space while maintaining the property that nearby points in 2D remain nearby in the 1D mapping