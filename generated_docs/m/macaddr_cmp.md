# macaddr_cmp

## Location
[src/backend/utils/adt/mac.c:197-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L197-L209)

## Overview
PostgreSQL function that compares two MAC addresses and returns an integer indicating their relative order for sorting purposes.

## Definition

```c
Datum
macaddr_cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
The macaddr_cmp function is a PostgreSQL built-in function that implements three-way comparison for MAC address data types. It takes two MAC address arguments through the PostgreSQL function calling convention and returns an integer value: negative if the first MAC address is less than the second, zero if they are equal, and positive if the first is greater than the second. The function delegates the actual comparison logic to macaddr_cmp_internal, which compares MAC addresses by first comparing the high-order 24 bits (hibits) and then the low-order 24 bits (lobits) if the high-order bits are equal.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing two macaddr pointers
  - Argument 0: First MAC address to compare (macaddr*)
  - Argument 1: Second MAC address to compare (macaddr*)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR_P (extracts macaddr argument from function args)
  - [macaddr_cmp_internal](macaddr_cmp_internal.md) (performs the actual comparison logic)
  - PG_RETURN_INT32 (returns 32-bit integer result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function follows PostgreSQL's standard comparison function interface for B-tree indexing and sorting operations
- The comparison is performed hierarchically: first by the high-order 24 bits of the MAC address, then by the low-order 24 bits
- Returns values compatible with qsort-style comparison functions (-1, 0, 1)
- Located in src/backend/utils/adt/mac.c:197-209

## Simplified Source

```c
// Simplified version of macaddr_cmp
Datum macaddr_cmp(PG_FUNCTION_ARGS) {
    macaddr *a1 = PG_GETARG_MACADDR_P(0);
    macaddr *a2 = PG_GETARG_MACADDR_P(1);

    // Delegate to internal comparison function
    PG_RETURN_INT32(macaddr_cmp_internal(a1, a2));
}
```