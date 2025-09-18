# MVNDistinct

## Location
src/include/statistics/statistics.h: 34 - 40

## Overview
MVNDistinct is a structure that represents multivariate n-distinct statistics for PostgreSQL's extended statistics system, comprising all possible combinations of columns with their distinct value counts.

## Definition


## Detailed Description
MVNDistinct is the core data structure for storing multivariate n-distinct statistics in PostgreSQL's extended statistics framework. It contains information about the number of distinct value combinations across multiple columns, which is crucial for query optimization when dealing with correlated columns. The structure uses a flexible array member to store a variable number of MVNDistinctItem entries, each representing statistics for a specific combination of attributes.

The structure is designed to be serializable and includes magic number validation for data integrity verification. It supports the BASIC type of n-distinct statistics and can handle up to STATS_MAX_DIMENSIONS (8) attributes in combinations.

## Parameters / Member Variables
- : Magic constant marker (STATS_NDISTINCT_MAGIC = 0xA352BFA4) used for structure validation and identification
- : Type identifier for the n-distinct statistic, currently supports STATS_NDISTINCT_TYPE_BASIC (1)
- : Number of MVNDistinctItem entries stored in the items array
- : Flexible array of MVNDistinctItem structures, each containing n-distinct information for a specific column combination

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array)
  - MVNDistinctItem (component structure)
  - STATS_NDISTINCT_MAGIC (magic constant)
  - STATS_NDISTINCT_TYPE_BASIC (type constant)

- Called from (representative examples):
  - statext_ndistinct_build (builds MVNDistinct statistics)
  - statext_ndistinct_serialize (serializes structure for storage)
  - statext_ndistinct_deserialize (deserializes from storage)
  - estimate_multivariate_ndistinct (uses for query optimization)
  - BuildRelationExtStatistics (part of statistics collection)

## Notes and Other Information
- Part of PostgreSQL's extended statistics system introduced to handle correlated columns
- Used by the query planner to make better cardinality estimates for multi-column predicates
- The structure is stored in the pg_statistic_ext_data system catalog
- Maximum supported dimensions is limited by STATS_MAX_DIMENSIONS (8 attributes)
- The flexible array member pattern allows for efficient memory usage with variable numbers of items
- Essential for improving query performance on tables with correlated columns