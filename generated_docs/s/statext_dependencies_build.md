# statext_dependencies_build

## Location
src/backend/statistics/dependencies.c: 348 - 443

## Overview
Detects functional dependencies between groups of columns by generating all possible subsets and computing the degree of validity for each dependency relationship.

## Definition


## Detailed Description
This function is the main entry point for building functional dependency statistics in PostgreSQL. It systematically explores all possible functional dependencies by:

1. Generating all possible column combinations from size 2 up to all available columns
2. For each combination, testing if the first (k-1) columns functionally determine the last column
3. Using dependency_degree() to compute a confidence score for each potential dependency
4. Collecting only dependencies with non-zero confidence scores
5. Returning a complete MVDependencies structure containing all discovered dependencies

The function uses a DependencyGenerator to enumerate all possible attribute combinations efficiently. It employs a separate memory context for intermediate calculations to manage memory usage during the intensive dependency degree computations.

## Parameters / Member Variables
- : StatsBuildData structure containing sample data and column metadata for statistics computation

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - DependencyGenerator_init  
  - DependencyGenerator_next
  - DependencyGenerator_free
  - dependency_degree
  - MemoryContextReset
  - MemoryContextDelete
  - repalloc
- Called from:
  - BuildRelationExtStatistics

## Notes and Other Information
- Requires at least 2 columns (data->nattnums >= 2) to function properly
- Uses a dedicated memory context 'dependency_degree cxt' for intermediate calculations
- Only stores dependencies with degree > 0.0 to avoid storing completely invalid relationships  
- The resulting MVDependencies structure uses magic numbers (STATS_DEPS_MAGIC) and type identifiers (STATS_DEPS_TYPE_BASIC) for validation
- Part of PostgreSQL's extended statistics framework for multivariate analysis
- For n columns, generates n*(n-1) two-column dependencies plus higher-order combinations
- Memory management is carefully handled with context switching and resets during intensive computations