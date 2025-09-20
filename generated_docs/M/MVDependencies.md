# MVDependencies

## Location
[src/include/statistics/statistics.h:57-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/statistics/statistics.h#L57-L63)

## Overview
MVDependencies is a structure that represents multivariate functional dependencies in PostgreSQL's extended statistics system, storing relationships where values in one set of columns determine values in another set.

## Definition

```c
typedef struct MVDependencies
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	uint32		ndeps;			/* number of dependencies */
	MVDependency *deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependencies;
```
## Detailed Description
MVDependencies is the container structure for storing multivariate functional dependency statistics in PostgreSQL's extended statistics framework. Functional dependencies represent relationships where the values in one set of columns uniquely determine the values in another set of columns (e.g., if column A determines column B, then knowing A's value allows predicting B's value).

This structure is crucial for query optimization as it helps the planner understand column correlations and make better selectivity estimates for complex WHERE clauses involving multiple correlated columns. Each MVDependency within the structure contains information about a specific functional dependency relationship with its degree of validity.

The structure uses a flexible array of pointers to MVDependency structures, allowing efficient storage and access to variable numbers of dependency relationships.

## Parameters / Member Variables
- `magic`: Magic constant marker (STATS_DEPS_MAGIC = 0xB4549A2C) used for structure validation and serialization integrity
- `type`: Type identifier for the dependency statistic, currently supports STATS_DEPS_TYPE_BASIC (1)
- `ndeps`: Number of MVDependency structures stored in the deps array
- `*deps[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of pointers to MVDependency structures, each representing a specific functional dependency
## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array)
  - MVDependency (component structure for individual dependencies)
  - STATS_DEPS_MAGIC (magic constant)
  - STATS_DEPS_TYPE_BASIC (type constant)

- Called from (representative examples):
  - [statext_dependencies_build](../s/statext_dependencies_build.md) (builds MVDependencies statistics)
  - [statext_dependencies_serialize](../s/statext_dependencies_serialize.md) (serializes structure for storage)
  - [statext_dependencies_deserialize](../s/statext_dependencies_deserialize.md) (deserializes from storage)
  - [dependencies_clauselist_selectivity](../d/dependencies_clauselist_selectivity.md) (uses for selectivity estimation)
  - DependencyGenerator (generates dependency combinations)
  - [find_strongest_dependency](../f/find_strongest_dependency.md) (analyzes dependency strength)

## Notes and Other Information
- Part of PostgreSQL's extended statistics system for handling correlated columns
- Used by the query planner to improve cardinality estimates for multi-column predicates
- Stored in the pg_statistic_ext_data system catalog as serialized bytea
- Each dependency has a degree value (0-1) indicating the strength/validity of the relationship
- Essential for optimizing queries on tables with functional relationships between columns
- The degree of dependency influences how much the planner trusts the relationship for estimation
- Maximum supported dimensions is limited by STATS_MAX_DIMENSIONS (8 attributes)
- Particularly useful for normalized database schemas where foreign key relationships create dependencies