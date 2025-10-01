# generate_dependencies_recurse

## Location
[src/backend/statistics/dependencies.c:91-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L91-L156)

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
  - [generate_dependencies_recurse](generate_dependencies_recurse.md) (recursive self-call)
  - [repalloc](../r/repalloc.md) (memory reallocation)
- Called from (representative examples):
  - [generate_dependencies](generate_dependencies.md)
  - [generate_dependencies_recurse](generate_dependencies_recurse.md) (recursive calls)

## Notes and Other Information
- This is a static function internal to the dependencies.c module
- Uses a recursive backtracking approach to systematically generate all valid dependencies
- Memory management is handled dynamically using repalloc as dependencies are discovered
- The algorithm ensures no duplicate dependencies like (a,b=>c) and (b,a=>c) by maintaining ascending order in the first k-1 positions
- Located at src/backend/statistics/dependencies.c:91-156

## Simplified Source

```c
static void
generate_dependencies_recurse(DependencyGenerator state, int index,
                              AttrNumber start, AttrNumber *current)
{
    // Handle first (k-1) elements: generate in ascending order
    if (index < (state->k - 1))
    {
        for (AttrNumber i = start; i < state->n; i++)
        {
            current[index] = i;
            generate_dependencies_recurse(state, (index + 1), (i + 1), current);
        }
    }
    else
    {
        // Handle last element: can be any attribute not in first (k-1) positions
        for (int i = 0; i < state->n; i++)
        {
            current[index] = i;

            // Check if this attribute already appears in first (k-1) positions
            bool match = false;
            for (int j = 0; j < index; j++)
            {
                if (current[j] == i)
                {
                    match = true;
                    break;
                }
            }

            // If unique, store this dependency
            if (!match)
            {
                state->dependencies = (AttrNumber *) repalloc(state->dependencies,
                    state->k * (state->ndependencies + 1) * sizeof(AttrNumber));
                memcpy(&state->dependencies[(state->k * state->ndependencies)],
                       current, state->k * sizeof(AttrNumber));
                state->ndependencies++;
            }
        }
    }
}
```