# network_fast_cmp

## Location
[src/backend/utils/adt/network.c:473-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L473-L487)

## Overview
Optimized comparison function specifically designed for PostgreSQL's SortSupport system to provide fast network address comparisons during sorting operations.

## Definition

```c
static int
network_fast_cmp(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
This function serves as the fast comparison routine for PostgreSQL's SortSupport optimization framework when sorting network address data types. It acts as a specialized wrapper around , but operates directly on Datum values rather than through the standard PostgreSQL function call interface.

The function is specifically optimized for high-performance sorting scenarios where:
- Direct Datum-to-Datum comparison is needed
- Function call overhead must be minimized
- Integration with SortSupport's optimization strategies is required

Unlike the public  function which handles PostgreSQL's function calling conventions, this function operates at a lower level, directly extracting inet pointers from Datum values and delegating to the core comparison logic. This design eliminates overhead associated with PostgreSQL's general-purpose function call mechanism during intensive sorting operations.

The function is marked as , indicating it's an internal implementation detail of the network data type's sort optimization system.

## Parameters / Member Variables
- `x`: First network address as a Datum value
- `y`: Second network address as a Datum value
- `ssup`: SortSupport context (unused but required by the SortSupport interface)
## Dependencies
- Functions called/Symbols referenced:
  -  (Datum to inet pointer conversion macro)
  -  (core comparison logic)

- Called from (representative examples):
  -  (as primary comparator)
  -  (as full comparator for abbreviation fallback)

## Notes and Other Information
- Returns negative value if x < y, zero if equal, positive if x > y
- Designed specifically for SortSupport optimization - not for general use
- The  parameter is present for interface compliance but not used in the implementation
- Provides significant performance benefits during large-scale sorting operations on network data
- Part of PostgreSQL's advanced sort optimization infrastructure
- Marked as  - internal function not exposed outside network.c
- Located in 
- Essential component of the network data type's sort performance optimization strategy