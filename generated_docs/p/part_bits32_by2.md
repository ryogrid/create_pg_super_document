# part_bits32_by2

## Location
src/backend/access/gist/gistproc.c: 1586 - 1602

## Overview
A static utility function that performs bit interleaving by spreading the bits of a 32-bit unsigned integer across a 64-bit result, with zeros inserted between each original bit.

## Definition
```c
static uint64 part_bits32_by2(uint32 x)
```

## Detailed Description
This function implements the bit-spreading operation required for Morton code (Z-order) computation. It takes a 32-bit unsigned integer and spreads its bits across a 64-bit result by inserting a zero between each consecutive bit. The operation is performed through a series of parallel bit manipulation steps that progressively spread the bits further apart. This technique is a fundamental building block for creating Z-order curves used in spatial indexing, where bits from two coordinates are interleaved to create a single value that preserves spatial locality.

## Parameters / Member Variables
- `x`: A 32-bit unsigned integer whose bits are to be spread with zeros

## Dependencies
- Functions called/Symbols referenced:
  - UINT64CONST (macro for 64-bit constants)
- Called from:
  - [point_zorder_internal](point_zorder_internal.md) (used twice for X and Y coordinate bit spreading)

## Notes and Other Information
- Uses a series of parallel bit manipulation operations with specific bitmasks
- The algorithm works by repeatedly doubling the spacing between bits through left shifts and masking
- Each step in the sequence doubles the bit spacing: 1→2→4→8→16→32
- The final result has every other bit set to the original bits, with zeros in between
- Essential component of the Morton code generation process for spatial indexing
- The five-step process efficiently spreads 32 bits across 64 positions using bitwise operations
- Bitmasks used: 0x0000FFFF0000FFFF, 0x00FF00FF00FF00FF, 0x0F0F0F0F0F0F0F0F, 0x3333333333333333, 0x5555555555555555