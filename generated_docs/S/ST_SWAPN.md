# ST_SWAPN

## Location
src/include/lib/sort_template.h: 282 - 291

## Overview
ST_SWAPN is a macro that generates a type-specific function name for swapping N consecutive elements between two arrays in PostgreSQL's sort template system.

## Definition
```c
#define ST_SWAPN ST_MAKE_NAME(ST_SORT, swapn)
```

## Detailed Description
ST_SWAPN is a preprocessor macro defined in the sort template header that creates a unique function name for the multi-element swapping function. This macro is part of PostgreSQL's generic sort template system and generates a function that can efficiently swap multiple consecutive elements between two arrays or memory locations. This is particularly useful in sorting algorithms when dealing with records or multi-element data structures that need to be moved as units.

The actual function it refers to has the signature:
```c
static inline void
ST_SWAPN(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b, size_t n)
```

The function implementation iterates through n elements and swaps each pair using the ST_SWAP function. It supports potential interruption checking if ST_CHECK_FOR_INTERRUPTS is defined, allowing long-running operations to be cancelled.

## Parameters / Member Variables
- `ST_SORT`: The base name prefix for the sort implementation being generated
- `swapn`: The suffix that identifies this as the multi-element swapping function

## Dependencies
- Functions called/Symbols referenced:
  - ST_MAKE_NAME (macro for generating unique function names)
  - ST_SORT (base sort name prefix)
  - ST_CHECK_FOR_INTERRUPTS (optional, for supporting interruption during long operations)
- Called from (representative examples):
  - DO_SWAPN (wrapper macro that invokes the generated function)

## Notes and Other Information
- This macro is only defined when ST_DEFINE is set, indicating template instantiation
- The actual implementation uses ST_SWAP internally to perform individual element swaps
- Supports interruption checking for long-running swap operations if configured
- Used when sorting algorithms need to move blocks of related data together
- The function is declared as static inline for optimal performance
- More efficient than multiple individual swaps when dealing with multi-element records
- Part of the template-based approach that generates optimized functions for different data types