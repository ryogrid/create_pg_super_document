# macaddr_cmp_internal

## Location
src/backend/utils/adt/mac.c: 182 - 196

## Overview
This static function provides the core comparison logic for MAC addresses, comparing them lexicographically by treating them as two 24-bit integers (high and low portions).

## Definition
```c
static int macaddr_cmp_internal(macaddr *a1, macaddr *a2)
```

## Detailed Description
The `macaddr_cmp_internal` function implements the fundamental comparison algorithm for MAC addresses in PostgreSQL. It performs a lexicographic comparison by splitting each 48-bit MAC address into two 24-bit segments: the high-order bits (first 3 octets) and low-order bits (last 3 octets). The comparison follows standard three-way comparison semantics, returning negative, zero, or positive values to indicate less-than, equal-to, or greater-than relationships.

The function first compares the high-order 24 bits (octets a, b, c), and only if they are equal does it proceed to compare the low-order 24 bits (octets d, e, f). This approach ensures efficient comparison while maintaining proper lexicographic ordering of MAC addresses.

## Parameters / Member Variables
- `a1`: Pointer to the first macaddr structure for comparison
- `a2`: Pointer to the second macaddr structure for comparison

## Dependencies
- Functions called/Symbols referenced:
  - `hibits`: Macro that extracts the high-order 24 bits (octets a, b, c) as an unsigned long
  - `lobits`: Macro that extracts the low-order 24 bits (octets d, e, f) as an unsigned long
- Called from (representative examples):
  - [macaddr_cmp](macaddr_cmp.md): Public comparison function at src/backend/utils/adt/mac.c:202
  - [macaddr_lt](macaddr_lt.md): Less-than comparison at src/backend/utils/adt/mac.c:215
  - [macaddr_le](macaddr_le.md): Less-than-or-equal comparison at src/backend/utils/adt/mac.c:224
  - [macaddr_eq](macaddr_eq.md): Equality comparison at src/backend/utils/adt/mac.c:233
  - [macaddr_ge](macaddr_ge.md): Greater-than-or-equal comparison at src/backend/utils/adt/mac.c:242
  - [macaddr_gt](macaddr_gt.md): Greater-than comparison at src/backend/utils/adt/mac.c:251
  - [macaddr_ne](macaddr_ne.md): Not-equal comparison at src/backend/utils/adt/mac.c:260
  - [macaddr_fast_cmp](macaddr_fast_cmp.md): Fast comparison for sorting at src/backend/utils/adt/mac.c:405

## Notes and Other Information
- Returns -1 if a1 < a2, 0 if a1 == a2, and 1 if a1 > a2
- Uses efficient bit manipulation via hibits and lobits macros to avoid byte-by-byte comparison
- The hibits macro combines octets a, b, c as: `((a<<16)|(b<<8)|c)`
- The lobits macro combines octets d, e, f as: `((d<<16)|(e<<8)|f)`
- This function is the foundation for all MAC address comparison operations in PostgreSQL
- Static function, only accessible within the mac.c compilation unit
- Optimized for performance in sorting and indexing operations