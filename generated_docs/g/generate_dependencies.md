# generate_dependencies

## Location
[src/backend/statistics/dependencies.c:157-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L157-L172)

## Overview
A wrapper function that initiates the generation of all possible functional dependencies of size k from n attributes using a recursive algorithm.

## Definition
```c
static void generate_dependencies(DependencyGenerator state)
```

## Detailed Description
This function serves as the entry point for generating all k-permutations of n elements that represent functional dependencies. It allocates a working array to hold the current dependency being constructed, then calls the recursive helper function to perform the actual generation. The function handles memory management by allocating and freeing the temporary working space.

The dependencies generated follow the pattern where the first (k-1) elements are in ascending order and the last element can be any attribute not appearing in the first (k-1) positions, effectively generating all possible functional dependencies while avoiding duplicates.

## Parameters / Member Variables
- `state`: DependencyGenerator containing the generation parameters (k for dependency size, n for number of attributes) and storage for the generated dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [generate_dependencies_recurse](generate_dependencies_recurse.md) (recursive dependency generation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [DependencyGenerator_init](../D/DependencyGenerator_init.md)

## Notes and Other Information
- This is a static function internal to the dependencies.c module
- Acts as a simple wrapper around the recursive generation algorithm
- Handles memory management for the working array used during generation
- The generated dependencies are stored in the state->dependencies array
- All dependencies are generated in a single call - this is not an iterator-based approach
- Located at src/backend/statistics/dependencies.c:157-172

## Simplified Source

```c
static void
generate_dependencies(DependencyGenerator state)
{
    // Allocate working array for current dependency construction
    AttrNumber *current = (AttrNumber *) palloc0(sizeof(AttrNumber) * state->k);

    // Generate all k-permutations recursively
    generate_dependencies_recurse(state, 0, 0, current);

    // Clean up working array
    pfree(current);
}
```