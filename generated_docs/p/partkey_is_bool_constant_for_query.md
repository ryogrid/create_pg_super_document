# partkey_is_bool_constant_for_query

## Location
[src/backend/optimizer/path/pathkeys.c:842-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L842-L881)

## Overview
Determines if a partition key column is constrained to have a constant boolean value by the query's WHERE conditions, making it irrelevant for sort-order considerations.

## Definition

```c
static bool
partkey_is_bool_constant_for_query(RelOptInfo *partrel, int partkeycol)
```
## Detailed Description
This function addresses a specific optimization challenge with boolean partition key columns. When a partition key column is constrained to a constant value, it becomes irrelevant for sorting purposes. For non-boolean columns, this is typically handled through EquivalenceClasses created from WHERE clauses like "partkeycol = constant". However, boolean columns are special because expression preprocessing simplifies boolean conditions to "WHERE partkeycol" or "WHERE NOT partkeycol" instead of creating explicit equality comparisons.

The function checks if a boolean partition key column has such a boolean restriction clause applied to it, allowing the query planner to treat it as effectively constant for pathkey generation purposes. This ensures boolean partition keys work consistently with non-boolean partition keys in terms of sort optimization.

## Parameters / Member Variables
- `*partrel`: RelOptInfo for the partitioned relation being analyzed
- `partkeycol`: Index of the partition key column to check (0-based)
## Dependencies
- Functions called/Symbols referenced:
  - IsBuiltinBooleanOpfamily
  - [matches_boolean_partition_clause](../m/matches_boolean_partition_clause.md)
  - [PartitionScheme](../P/PartitionScheme.md) (type)
- Called from (representative examples):
  - [build_partition_pathkeys](../b/build_partition_pathkeys.md)

## Notes and Other Information
- This is a static function, only used within the pathkeys.c module
- Only works with built-in boolean operator families since partitioning currently only supports built-in access methods
- Ignores pseudoconstant restriction clauses as they won't provide useful matches
- Part of PostgreSQL's partition-aware query optimization infrastructure
- Enables consistent handling of boolean vs. non-boolean partition key columns in pathkey generation
- Returns true if a matching boolean restriction clause is found, false otherwise

## Simplified Source

```c
static bool
partkey_is_bool_constant_for_query(RelOptInfo *partrel, int partkeycol)
{
    PartitionScheme partscheme = partrel->part_scheme;
    ListCell *lc;

    // Only boolean partition keys can match boolean restrictions
    if (!IsBuiltinBooleanOpfamily(partscheme->partopfamily[partkeycol]))
        return false;

    // Check each WHERE clause restriction for this partitioned relation
    foreach(lc, partrel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        // Skip pseudoconstant clauses (they don't constrain partition keys)
        if (rinfo->pseudoconstant)
            continue;

        // Check if this restriction clause matches the boolean partition key
        if (matches_boolean_partition_clause(rinfo, partrel, partkeycol))
            return true;
    }

    return false;
}
```