# DependencyGenerator_init

## Location
src/backend/statistics/dependencies.c: 173 - 195

## Overview
Initializes a DependencyGenerator by allocating memory, setting up state variables, and pre-generating all possible functional dependencies of size k from n attributes.

## Definition
```c
static DependencyGenerator DependencyGenerator_init(int n, int k)
```

## Detailed Description
This constructor function creates and initializes a DependencyGenerator object. It performs validation to ensure n >= k and k > 0, then allocates memory for both the generator state and an initial dependencies array. The function takes an eager approach by pre-generating all possible dependencies immediately rather than generating them on-demand.

The initialization process involves:
1. Parameter validation using Assert
2. Memory allocation for the DependencyGeneratorData structure
3. Initial memory allocation for the dependencies array
4. Setting up state variables (ndependencies=0, current=0, k, n)
5. Calling generate_dependencies() to populate all possible dependencies

This design choice trades memory usage for simplicity and performance predictability.

## Parameters / Member Variables
- `n`: Number of possible attributes to choose from
- `k`: Size of each dependency (number of attributes in each dependency)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (parameter validation)
  - palloc0 (zero-initialized memory allocation)
  - palloc (memory allocation)
  - generate_dependencies (dependency generation)
- Called from (representative examples):
  - statext_dependencies_build

## Notes and Other Information
- This is a static function internal to the dependencies.c module
- Uses eager evaluation - all dependencies are generated at initialization time
- Requires n >= k and k > 0, enforced by Assert
- Returns a fully populated DependencyGenerator ready for iteration
- The initial dependencies array allocation may be too small and gets expanded by repalloc during generation
- Memory allocated here must be freed later using DependencyGenerator_free
- Located at src/backend/statistics/dependencies.c:173-195