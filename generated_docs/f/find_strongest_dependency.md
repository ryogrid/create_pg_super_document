# find_strongest_dependency

## Location
[src/backend/statistics/dependencies.c:929-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L929-L1013)

## Overview
Finds the strongest functional dependency from a collection of dependencies that can be fully matched against a given set of attribute numbers, prioritizing dependencies with more attributes and higher degrees of validity.

## Definition

```c
static MVDependency *
find_strongest_dependency(MVDependencies **dependencies, int ndependencies,
						  Bitmapset *attnums)
```
## Detailed Description
This function implements the selection logic for applying functional dependencies during selectivity estimation. It searches through all available dependencies to find the "strongest" one that can be applied to the current set of equality clauses. The strength of a dependency is determined by a hierarchical criteria:

1. **Full coverage**: The dependency must have all its attributes covered by equality clauses
2. **Attribute count**: Dependencies with more attributes are preferred (eliminate more redundant conditions)
3. **Degree of validity**: Among dependencies with the same attribute count, those with higher degrees are preferred

The function performs cheap checks first (attribute count comparisons) before expensive operations (full matching verification) to optimize performance. This selection strategy ensures that the most redundant conditions are eliminated first, leading to more accurate selectivity estimates.

## Parameters / Member Variables
- : Array of pointers to MVDependencies structures containing functional dependencies
- : Number of dependency collections in the array
- : Bitmapset representing attributes that have equality clauses

## Dependencies
- Functions called/Symbols referenced:
  - [bms_num_members](../b/bms_num_members.md)
  - [dependency_is_fully_matched](../d/dependency_is_fully_matched.md)
- Types used:
  - [MVDependencies](../M/MVDependencies.md)
  - MVDependency
- Called from (representative examples):
  - DependencyGenerator
  - [dependencies_clauselist_selectivity](../d/dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Returns NULL if no dependency can be fully matched against the provided attributes
- The algorithm prioritizes dependencies that eliminate the most redundant conditions first
- Performs optimization by checking cheaper criteria (attribute counts, degrees) before expensive matching operations
- The selection strategy directly impacts the accuracy of selectivity estimates in query planning
- Dependencies with more attributes are always preferred regardless of their degree of validity

## Simplified Source

```c
static MVDependency *
find_strongest_dependency(MVDependencies **dependencies, int ndependencies,
                          Bitmapset *attnums)
{
    int i, j;
    MVDependency *strongest = NULL;
    int nattnums = bms_num_members(attnums);

    // Iterate through all dependency collections
    for (i = 0; i < ndependencies; i++) {
        for (j = 0; j < dependencies[i]->ndeps; j++) {
            MVDependency *dependency = dependencies[i]->deps[j];

            // Skip if dependency has more attributes than available clauses
            if (dependency->nattributes > nattnums)
                continue;

            // Compare with current strongest dependency
            if (strongest) {
                // Prefer dependencies with more attributes
                if (dependency->nattributes < strongest->nattributes)
                    continue;

                // For same attribute count, prefer higher degree
                if (strongest->nattributes == dependency->nattributes &&
                    strongest->degree > dependency->degree)
                    continue;
            }

            // Check if dependency is fully matched (expensive check last)
            if (dependency_is_fully_matched(dependency, attnums))
                strongest = dependency;
        }
    }

    return strongest;
}
```