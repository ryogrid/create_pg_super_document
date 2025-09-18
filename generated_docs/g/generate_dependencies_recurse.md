# generate_dependencies_recurse

## Location
src/backend/statistics/dependencies.c: 91 - 156

## Overview
A recursive helper function that generates all possible functional dependencies of size k from n attributes, where the first (k-1) elements are in ascending order and the last element can be any attribute not in the first (k-1) positions.

## Definition
```c
static void generate_dependencies_recurse(DependencyGenerator state, int index,
                                        AttrNumber start, AttrNumber *current)
```

## Detailed Description
This function is the core recursive engine for generating functional dependencies. It operates by treating the first (k-1) elements differently from the last element:

- **For positions 0 to k-2**: Generates combinations in ascending order recursively to avoid duplicates like (a,b=>c) and (b,a=>c)
- **For the last position (k-1)**: Tries all possible attribute numbers, ensuring the chosen attribute doesn't appear in the first (k-1) positions

When a valid dependency is found (last element doesn't match any of the first k-1 elements), it's stored in the state's dependencies array using repalloc to expand the array as needed.

## Parameters / Member Variables
- `state`: DependencyGenerator containing the generation state (k, n, dependencies array, etc.)
- `index`: Current position in the dependency being generated (0 to k-1)
- `start`: Starting attribute number for the current position (ensures ascending order)
- `current`: Working array holding the current dependency being constructed

## Dependencies
- Functions called/Symbols referenced:
  - generate_dependencies_recurse (recursive self-call)
  - repalloc (memory reallocation)
- Called from (representative examples):
  - generate_dependencies
  - generate_dependencies_recurse (recursive calls)

## Notes and Other Information
- This is a static function internal to the dependencies.c module
- Uses a recursive backtracking approach to systematically generate all valid dependencies
- Memory management is handled dynamically using repalloc as dependencies are discovered
- The algorithm ensures no duplicate dependencies like (a,b=>c) and (b,a=>c) by maintaining ascending order in the first k-1 positions
- Located at src/backend/statistics/dependencies.c:91-156