# atcomp

## Location
[src/timezone/zic.c:2037-2044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2037-L2044)

## Overview
A static comparison function used for sorting `attype` structures by their `at` field values in ascending order.

## Definition
```c
static int atcomp(const void *avp, const void *bvp)
```

## Detailed Description
The `atcomp` function is a comparison function designed to be used with the standard C library's `qsort` function. It compares two `attype` structures based on their `at` field values (which are of type `zic_t`). The function follows the standard comparison function contract: it returns a negative value if the first element should come before the second, zero if they are equal, and a positive value if the first element should come after the second. This enables sorting of timezone transition data in chronological order.

## Parameters / Member Variables
- `avp`: A void pointer to the first `attype` structure to be compared
- `bvp`: A void pointer to the second `attype` structure to be compared

## Dependencies
- Functions called/Symbols referenced:
  - zic_t (type definition)
  - attype (structure type for timezone transition data)
- Called from (representative examples):
  - [writezone](../w/writezone.md) (likely used with qsort for sorting timezone transitions)

## Notes and Other Information
- This function is designed to be used as a callback with qsort() for sorting arrays of attype structures
- The comparison is based on the `at` field which likely represents timestamps or transition times
- Returns -1 if a < b, 0 if a == b (though this case returns 0 implicitly), and 1 if a > b
- This function is static and only accessible within the zic.c compilation unit
- Part of PostgreSQL's timezone data compilation infrastructure for organizing transition times
- The function uses void pointers to match the qsort callback signature requirements