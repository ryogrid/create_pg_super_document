# macaddr8_cmp_internal

## Location
[src/backend/utils/adt/mac8.c:310-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L310-L324)

## Overview
Internal comparison function for macaddr8 (EUI-64) MAC addresses that returns a tri-state comparison result for sorting operations.

## Definition
```c
static int32 macaddr8_cmp_internal(macaddr8 *a1, macaddr8 *a2)
```

## Detailed Description
This static function performs a lexicographic comparison of two macaddr8 structures by comparing their high-order and low-order bits separately. It first compares the upper 4 bytes (a, b, c, d) as a 32-bit value, and if they are equal, then compares the lower 4 bytes (e, f, g, h). The function is designed specifically for sorting algorithms and provides the foundation for all macaddr8 comparison operators.

The comparison is performed using two utility macros:
- `hibits(addr)`: Extracts the high-order 32 bits as `((addr->a<<24) | (addr->b<<16) | (addr->c<<8) | addr->d)`
- `lobits(addr)`: Extracts the low-order 32 bits as `((addr->e<<24) | (addr->f<<16) | (addr->g<<8) | addr->h)`

## Parameters / Member Variables
- `a1`: Pointer to the first macaddr8 structure to compare
- `a2`: Pointer to the second macaddr8 structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - hibits (macro for extracting high-order bits)
  - lobits (macro for extracting low-order bits)
  - [macaddr8](macaddr8.md) (structure type)
- Called from (representative examples):
  - [macaddr8_cmp](macaddr8_cmp.md)
  - [macaddr8_lt](macaddr8_lt.md)
  - [macaddr8_le](macaddr8_le.md)
  - [macaddr8_eq](macaddr8_eq.md)
  - [macaddr8_ge](macaddr8_ge.md)
  - [macaddr8_gt](macaddr8_gt.md)
  - [macaddr8_ne](macaddr8_ne.md)

## Notes and Other Information
- Returns -1 if a1 < a2, 0 if a1 == a2, and 1 if a1 > a2
- This is a static function, only accessible within the mac8.c file
- Serves as the core comparison logic for all macaddr8 comparison operators
- Uses efficient bit manipulation to compare 8-byte MAC addresses as two 32-bit values
- The comparison follows standard lexicographic ordering based on byte values

## Simplified Source

```c
static int32 macaddr8_cmp_internal(macaddr8 *a1, macaddr8 *a2) {
    // Compare high-order 32 bits first (bytes a,b,c,d)
    if (hibits(a1) < hibits(a2))
        return -1;
    else if (hibits(a1) > hibits(a2))
        return 1;

    // High bits equal, compare low-order 32 bits (bytes e,f,g,h)
    else if (lobits(a1) < lobits(a2))
        return -1;
    else if (lobits(a1) > lobits(a2))
        return 1;

    // Both parts equal
    else
        return 0;
}
```