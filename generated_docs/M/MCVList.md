# MCVList

## Location
[src/include/statistics/statistics.h:87-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/statistics/statistics.h#L87-L95)

## Overview
MCVList is a container structure that holds a collection of multivariate most-common values (MCV) items, representing the most frequent combinations of values across multiple correlated columns.

## Definition


## Detailed Description
MCVList is the primary data structure for storing multivariate most-common values statistics in PostgreSQL's extended statistics system. It contains an array of MCVItem structures, each representing a frequently occurring combination of values across multiple columns. This structure is essential for query optimization when dealing with correlated columns that have common value patterns.

The structure includes metadata about the dimensions being tracked, data types for each dimension, and the actual MCV items. The magic number and type fields provide structure validation and versioning support. The ndimensions field indicates how many columns are involved in the statistics, while the types array stores the PostgreSQL type OIDs for each column to ensure proper data interpretation.

This information enables the query planner to make accurate selectivity estimates for multi-column predicates, especially when columns exhibit correlation patterns that would be missed by independent column statistics.

## Parameters / Member Variables
- : Magic constant marker (STATS_MCV_MAGIC = 0xE1A651C2) used for structure validation and serialization integrity
- : Type identifier for the MCV list, currently supports STATS_MCV_TYPE_BASIC (1)
- : Number of MCVItem entries stored in the items array (limited by STATS_MCVLIST_MAX_ITEMS)
- : Number of columns/attributes represented in this MCV list (limited by STATS_MAX_DIMENSIONS = 8)
- : Array of PostgreSQL type OIDs for each dimension, used for proper value interpretation and serialization
- : Flexible array of MCVItem structures containing the actual most-common value combinations and their frequencies

## Dependencies
- Functions called/Symbols referenced:
  - STATS_MAX_DIMENSIONS (maximum dimension limit constant)
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array)
  - [MCVItem](MCVItem.md) (component structure for individual MCV entries)
  - STATS_MCV_MAGIC (magic constant)
  - STATS_MCV_TYPE_BASIC (type constant)
  - AttrNumber (PostgreSQL attribute number type)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - [statext_mcv_build](../s/statext_mcv_build.md) (constructs MCVList from sample data)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (serializes MCVList for storage)
  - [statext_mcv_deserialize](../s/statext_mcv_deserialize.md) (deserializes MCVList from storage)
  - [statext_mcv_load](../s/statext_mcv_load.md) (loads MCVList from system catalogs)
  - mcv_clauselist_selectivity (uses for selectivity estimation)
  - [pg_stats_ext_mcvlist_items](../p/pg_stats_ext_mcvlist_items.md) (exposes through system views)
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (part of statistics collection)

## Notes and Other Information
- Core component of PostgreSQL's extended statistics system for multivariate analysis
- Used by the query planner to improve cardinality estimates for complex multi-column predicates
- Stored in the pg_statistic_ext_data system catalog as serialized bytea
- Maximum dimensions limited to STATS_MAX_DIMENSIONS (8 attributes)
- Maximum items limited to STATS_MCVLIST_MAX_ITEMS (MAX_STATISTICS_TARGET)
- Essential for optimizing queries on tables with correlated columns that exhibit common value patterns
- The flexible array pattern allows efficient memory usage with variable numbers of MCV items
- Type information enables proper handling of different data types across dimensions
- Particularly effective for improving estimates on foreign key relationships and other correlated column scenarios
- Works in conjunction with MVNDistinct and MVDependencies to provide comprehensive multivariate statistics