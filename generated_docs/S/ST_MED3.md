# ST_MED3

## Location
[src/include/lib/sort_template.h:261-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/sort_template.h#L261-L272)

## Overview
ST_MED3 is a macro that generates a type-specific function name for the median-of-three selection algorithm used in PostgreSQL's sort template system.

## Definition

```c
static pg_noinline ST_ELEMENT_TYPE *
ST_MED3(ST_ELEMENT_TYPE * a,
		ST_ELEMENT_TYPE * b,
		ST_ELEMENT_TYPE * c
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
```
## Detailed Description
ST_MED3 is a preprocessor macro defined in the sort template header that creates a unique function name for the median-of-three selection function. This macro is part of PostgreSQL's generic sort template system, which allows for type-specific sorting implementations while maintaining code reusability. The actual function it refers to implements the median-of-three algorithm, which is used to select a good pivot element for quicksort by finding the median value among three elements.

The generated function has the signature:


## Parameters / Member Variables
- : The base name prefix for the sort implementation being generated
- : The suffix that identifies this as the median-of-three function

## Dependencies
- Functions called/Symbols referenced:
  - ST_MAKE_NAME (macro for generating unique function names)
  - ST_SORT (base sort name prefix)
- Called from (representative examples):
  - DO_MED3 (wrapper macro that invokes the generated function)

## Notes and Other Information
- This macro is only defined when ST_DEFINE is set, indicating template instantiation
- The actual median-of-three implementation compares three elements and returns the median value
- Used internally by PostgreSQL's sorting algorithms to improve quicksort performance by selecting better pivot elements
- Part of the template-based approach that allows PostgreSQL to generate optimized sorting functions for different data types

## Simplified Source

```c
// Macro definition
#define ST_MED3 ST_MAKE_NAME(ST_SORT, med3)

// Implementation
static pg_noinline ST_ELEMENT_TYPE *
ST_MED3(ST_ELEMENT_TYPE *a, ST_ELEMENT_TYPE *b, ST_ELEMENT_TYPE *c
        ST_SORT_PROTO_COMPARE ST_SORT_PROTO_ARG) {

    // Find median of three elements using conditional expressions
    return DO_COMPARE(a, b) < 0 ?
        (DO_COMPARE(b, c) < 0 ? b : (DO_COMPARE(a, c) < 0 ? c : a))
        : (DO_COMPARE(b, c) > 0 ? b : (DO_COMPARE(a, c) < 0 ? a : c));
}
```