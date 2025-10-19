# ST_SWAP

## Location
[src/include/lib/sort_template.h:273-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/sort_template.h#L273-L281)

## Overview
ST_SWAP is a macro that generates a type-specific function name for the element swapping function used in PostgreSQL's sort template system.

## Definition
```c
#define ST_SWAP ST_MAKE_NAME(ST_SORT, swap)
```

## Detailed Description
ST_SWAP is a preprocessor macro defined in the sort template header that creates a unique function name for the element swapping function. This macro is part of PostgreSQL's generic sort template system, allowing for efficient, type-specific implementations of element swapping operations used in sorting algorithms. The generated function performs a simple swap operation between two elements of the templated type.

The actual function it refers to has the signature:
```c
static inline void
ST_SWAP(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b)
```

The function implementation performs a classic three-step swap using a temporary variable to exchange the values pointed to by the two parameters.

## Parameters / Member Variables
- `ST_SORT`: The base name prefix for the sort implementation being generated
- `swap`: The suffix that identifies this as the element swapping function

## Dependencies
- Functions called/Symbols referenced:
  - ST_MAKE_NAME (macro for generating unique function names)
  - ST_SORT (base sort name prefix)
- Called from (representative examples):
  - DO_SWAP (wrapper macro that invokes the generated function)
  - [ST_SWAPN](ST_SWAPN.md) (uses ST_SWAP internally for multi-element swapping)

## Notes and Other Information
- This macro is only defined when ST_DEFINE is set, indicating template instantiation
- The actual swap implementation uses a temporary variable to safely exchange two values
- Used extensively throughout PostgreSQL's sorting algorithms for element rearrangement
- The function is declared as static inline for optimal performance
- Part of the template-based approach that generates optimized swap functions for different data types and pointer types

## Simplified Source

```c
// Macro definition
#define ST_SWAP ST_MAKE_NAME(ST_SORT, swap)

// Implementation
static inline void
ST_SWAP(ST_POINTER_TYPE *a, ST_POINTER_TYPE *b) {
    ST_POINTER_TYPE tmp = *a;
    *a = *b;
    *b = tmp;
}
```