# pg_popcount_masked_choose

## Location
[src/port/pg_bitutils.c:204-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_bitutils.c#L204-L214)

## Overview
A static wrapper function that initializes the popcount function selection mechanism and then delegates to the appropriate optimized pg_popcount_masked implementation.

## Definition

```c
static uint64
pg_popcount_masked_choose(const char *buf, int bytes, bits8 mask)
```
## Detailed Description
This function serves as a bootstrap wrapper for the masked population count functionality in PostgreSQL. It's designed to be called only once through a function pointer mechanism. Upon first invocation, it calls choose_popcount_functions() to detect the available hardware capabilities and set up function pointers to the most appropriate implementation (fast/slow/AVX512). After initialization, it immediately delegates to the selected pg_popcount_masked implementation to perform the actual masked bit counting operation.

## Parameters / Member Variables
- `buf`: Pointer to the buffer containing the data to count bits in
- `bytes`: Number of bytes in the buffer to process
- `mask`: 8-bit mask specifying which bit positions to count

## Dependencies
- Functions called/Symbols referenced:
  - [choose_popcount_functions](../c/choose_popcount_functions.md)
  - [pg_popcount_masked](pg_popcount_masked.md)
- Called from (representative examples):
  - Used indirectly through function pointer initialization mechanism

## Notes and Other Information
- This is a static function used internally for the dynamic function selection mechanism
- Only called once during the first invocation of masked popcount functionality
- Part of PostgreSQL's runtime optimization strategy for bit manipulation operations
- After the first call, subsequent calls bypass this function entirely and go directly to the optimized implementation

## Simplified Source

```c
static uint64 pg_popcount_masked_choose(const char *buf, int bytes, bits8 mask) {
    // Initialize function selection mechanism
    choose_popcount_functions();

    // Delegate to selected masked popcount implementation
    return pg_popcount_masked(buf, bytes, mask);
}
```