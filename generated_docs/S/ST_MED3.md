# ST_MED3

## Location
src/include/lib/sort_template.h: 261 - 272

## Overview
ST_MED3 is a macro that generates a type-specific function name for the median-of-three selection algorithm used in PostgreSQL's sort template system.

## Definition


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