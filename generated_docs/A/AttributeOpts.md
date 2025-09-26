# AttributeOpts

## Location
src/include/utils/attoptcache.h: 19 - 24

## Overview
AttributeOpts is a structure that stores cached attribute-specific options in PostgreSQL, primarily containing statistics estimates for query optimization purposes.

## Definition
```c
typedef struct AttributeOpts
{
    int32     vl_len_;              /* varlena header (do not touch directly!) */
    float8    n_distinct;
    float8    n_distinct_inherited;
} AttributeOpts;
```

## Detailed Description
The AttributeOpts structure represents attribute options that are cached separately from the fixed-size portion of pg_attribute entries handled by the relcache. This structure is specifically designed to hold statistical information about table columns that can be explicitly set through the `ALTER TABLE ... ALTER COLUMN ... SET (option = value)` SQL syntax.

The structure follows PostgreSQL's varlena (variable-length array) format, making it suitable for storage in system catalogs and enabling efficient memory management. The primary purpose is to store column-level statistics that override PostgreSQL's automatic statistics collection, particularly for the query planner's cardinality estimation.

The attribute options are cached using a hash table (AttoptCacheHash) where the key consists of the relation OID and attribute number, and the cached data includes the parsed AttributeOpts structure. This caching mechanism improves performance by avoiding repeated parsing of the stored options from the pg_attribute.attoptions column.

## Parameters / Member Variables
- `vl_len_`: Variable-length array header required by PostgreSQL's varlena format. This field stores the total size of the structure and should not be manipulated directly by application code.
- `n_distinct`: Explicitly set estimate of the number of distinct values in this column. When set, this overrides PostgreSQL's automatic ANALYZE-generated statistics for cardinality estimation during query planning.
- `n_distinct_inherited`: Explicitly set estimate of the number of distinct values when considering inheritance hierarchies. This is used in partitioned tables where the column statistics need to account for values across all child tables.

## Dependencies
- Functions called/Symbols referenced:
  - int32 (PostgreSQL type alias)
  - float8 (PostgreSQL type alias)
- Called from (representative examples):
  - attribute_reloptions (src/backend/access/common/reloptions.c:2081-2087)
  - get_attribute_options (src/backend/utils/cache/attoptcache.c:108,126)
  - do_analyze_rel (src/backend/commands/analyze.c:553)
  - compute_expr_stats (src/backend/statistics/extended_stats.c:2195)

## Notes and Other Information
- The structure is part of PostgreSQL's reloptions (relation options) framework, specifically for attribute-level options
- Memory management follows PostgreSQL's palloc/pfree pattern, with cached entries stored in CacheMemoryContext
- Cache invalidation occurs when pg_attribute is updated, ensuring consistency between stored options and cached data
- The n_distinct values can be positive (absolute count) or negative (fraction of total rows), following PostgreSQL's statistics convention
- Currently used primarily for manual statistics tuning in cases where automatic ANALYZE produces suboptimal estimates
- The attribute options cache is not considered performance-critical and uses a simple "flush all on any change" invalidation strategy
- Introduced as part of PostgreSQL's infrastructure to support user-defined column statistics and query planner hints