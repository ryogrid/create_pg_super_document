# my_int128_compare

## Location
[src/tools/testint128.c:56-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/testint128.c#L56-L73)

## Overview
A control version of a comparator function that compares two 128-bit integer values and returns a standard comparison result.

## Definition

```c
static inline int
my_int128_compare(int128 x, int128 y)
```
## Detailed Description
This function provides a simple three-way comparison for 128-bit integers. It serves as a control implementation for testing purposes within the PostgreSQL test suite for 128-bit integer operations. The function follows standard comparison semantics, returning negative, zero, or positive values based on the relationship between the two input values.

## Parameters / Member Variables
- `x`: First 128-bit integer value to compare
- `y`: Second 128-bit integer value to compare
## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only built-in comparison operators)
- Called from (representative examples):
  - [main](main.md) (in src/tools/testint128.c at lines 143, 147, 157, 161)

## Notes and Other Information
- This is a static inline function defined in the testint128.c test utility
- Used as a reference implementation for validating other 128-bit integer comparison functions
- Returns -1 if x < y, 1 if x > y, and 0 if x == y following standard comparison function conventions

## Simplified Source

```c
static inline int my_int128_compare(int128 x, int128 y) {
    // Three-way comparison: return -1, 0, or 1
    if (x < y)
        return -1;
    if (x > y)
        return 1;
    return 0;
}
```