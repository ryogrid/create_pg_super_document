# StatisticExtInfo

## Location
src/include/nodes/pathnodes.h: 1266 - 1289

## Overview
StatisticExtInfo represents information about extended statistics for planning and optimization, corresponding to entries in the pg_statistic_ext system catalog that enhance PostgreSQL's selectivity estimation capabilities.

## Definition
```c
typedef struct StatisticExtInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;

    Oid         statOid;        /* OID of the statistics row */
    bool        inherit;        /* includes child relations */
    RelOptInfo *rel;           /* back-link to statistic's table */
    char        kind;          /* statistics kind of this entry */
    Bitmapset  *keys;          /* attnums of the columns covered */
    List       *exprs;         /* expressions */
} StatisticExtInfo;
```

## Detailed Description
StatisticExtInfo stores metadata about extended statistics objects that PostgreSQL uses to improve selectivity estimates for complex queries. Extended statistics go beyond simple column statistics to capture correlations, distinct value counts, and most common value combinations across multiple columns or expressions.

Each StatisticExtInfo node represents a specific kind of extended statistic (functional dependencies, n-distinct, MCV lists, etc.) for a particular set of columns or expressions. The structure enables the query planner to identify which extended statistics are available and applicable to specific query conditions, allowing for more accurate cardinality estimation in cases where column independence assumptions would be inadequate.

The planner uses these statistics during selectivity estimation to refine its understanding of data distribution patterns, particularly for multi-column predicates where traditional single-column statistics would provide poor estimates.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `statOid`: Object identifier of the corresponding pg_statistic_ext catalog entry
- `inherit`: Boolean flag indicating whether the statistics include child relations (for inheritance hierarchies)
- `rel`: Back-pointer to the RelOptInfo structure representing the table these statistics apply to
- `kind`: Character code identifying the specific type of extended statistic (e.g., dependencies, ndistinct, MCV)
- `keys`: Bitmapset containing the attribute numbers of columns covered by this statistic
- `exprs`: List of expressions covered by this statistic (for statistics on expressions rather than just columns)

## Dependencies
- Functions called/Symbols referenced:
  - RelOptInfo (structure for relation optimization info)
  - Bitmapset (bitmap data structure)
  - List (generic list structure)

- Called from (representative examples):
  - get_relation_statistics_worker (plancat.c:1404, 1418, 1432, 1446)
  - dependency_is_compatible_expression (dependencies.c:1321)
  - dependencies_clauselist_selectivity (dependencies.c:1569)
  - has_stats_of_kind (extended_stats.c:1124)
  - choose_best_statistics (extended_stats.c:1214, 1221)
  - statext_mcv_clauselist_selectivity (extended_stats.c:1754)
  - estimate_multivariate_ndistinct (selfuncs.c:3975, 3988)

## Notes and Other Information
- Each pg_statistic_ext row may be represented by multiple StatisticExtInfo nodes, one for each statistics kind computed
- Some statistics may have zero corresponding nodes if ANALYZE has not yet computed them
- The `rel` pointer uses read_write_ignore annotation to prevent infinite recursion during serialization
- Extended statistics are particularly valuable for queries with predicates on correlated columns where traditional independence assumptions fail
- The structure supports both column-based statistics (via `keys`) and expression-based statistics (via `exprs`)
- Statistics kinds include functional dependencies, n-distinct estimates, and most common value lists