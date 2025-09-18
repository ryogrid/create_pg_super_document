# DependencyGeneratorData

## Location
src/backend/statistics/dependencies.c: 56 - 63

## Overview
Internal state structure for generating dependency combinations in PostgreSQL statistics, used to iterate through k-permutations of n elements where order does not matter for the first (k-1) elements.

## Definition
```c
typedef struct DependencyGeneratorData
{
    int         k;              /* size of the dependency */
    int         n;              /* number of possible attributes */
    int         current;        /* next dependency to return (index) */
    AttrNumber  ndependencies;  /* number of dependencies generated */
    AttrNumber *dependencies;   /* array of pre-generated dependencies */
} DependencyGeneratorData;
```

## Detailed Description
`DependencyGeneratorData` is a structure that maintains the internal state for generating functional dependencies in PostgreSQL's multivariate statistics system. It is used to create combinations of attributes where dependencies like (a,b=>c) and (b,a=>c) are considered equivalent. The structure pre-generates all possible dependency combinations during initialization and provides an iterator interface to access them sequentially.

The generator works with the concept that for a dependency of size k from n possible attributes, the order of the first (k-1) elements does not matter, but the last element (the dependent attribute) is significant. This reflects the nature of functional dependencies where multiple attributes can determine another attribute.

## Parameters / Member Variables
- `k`: The size of each dependency combination (number of attributes involved in each dependency)
- `n`: The total number of possible attributes to choose from
- `current`: Index pointer tracking the next dependency to return when iterating through generated combinations
- `ndependencies`: The total count of dependency combinations that have been generated
- `dependencies`: Dynamically allocated array storing all pre-generated dependency combinations as AttrNumber values

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (int16 typedef for attribute numbers)
- Called from (representative examples):
  - DependencyGenerator (typedef pointer to this structure)
  - DependencyGenerator_init (initializes and allocates this structure)

## Notes and Other Information
- This structure is always accessed through the `DependencyGenerator` typedef, which is a pointer to `DependencyGeneratorData`
- All dependency combinations are pre-generated during initialization rather than being computed on-demand, trading memory for computational efficiency
- Located in `src/backend/statistics/dependencies.c:56-63`
- Part of PostgreSQL's extended statistics system for better cardinality estimation with correlated columns
- The structure supports the mathematical concept where dependencies are similar to k-permutations but with order-insensitive prefixes