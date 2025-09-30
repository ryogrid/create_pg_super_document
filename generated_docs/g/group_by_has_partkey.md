# group_by_has_partkey

## Location
[src/backend/optimizer/plan/planner.c:8084-8170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L8084-L8170)

## Overview
Determines whether all partition keys of a given relation are included in the GROUP BY clause with matching collation, enabling partitionwise aggregation optimization.

## Definition

```c
static bool
group_by_has_partkey(RelOptInfo *input_rel,
					 List *targetList,
					 List *groupClause)
```
## Detailed Description
This function checks if partitionwise aggregation can be performed by verifying that all partition key expressions are present in the GROUP BY clause. For partitionwise aggregation to be effective, every partition key must be included in the grouping operation to ensure that all rows of any given group come from the same partition.

The function performs a comprehensive comparison between partition key expressions and GROUP BY expressions, handling RelabelType nodes that may wrap expressions due to type coercions. It also enforces collation matching requirements - if both the partition key and grouping expression have valid collations, they must match exactly.

The algorithm iterates through each partition key attribute, then through all alternative expressions for that key (since a partition key can have multiple equivalent expressions), attempting to find a matching expression in the GROUP BY clause.

## Parameters / Member Variables
- : RelOptInfo for the partitioned input relation being examined
- : List of target list entries containing the expressions available for grouping
- : List of SortGroupClause nodes representing the GROUP BY clause

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md)
  - [exprCollation](../e/exprCollation.md)
  - [equal](../e/equal.md)
  - [RelabelType](../R/RelabelType.md) (type checking)
- Called from (representative examples):
  - [create_ordinary_grouping_paths](../c/create_ordinary_grouping_paths.md)

## Notes and Other Information
- The function requires the input relation to be partitioned (asserts on part_scheme)
- Returns false immediately if no partition key expressions exist
- Handles RelabelType nodes by unwrapping them to access the underlying expression
- Enforces strict collation matching when both partition and group expressions have valid collations
- All partition keys must be matched for the function to return true - even one missing key causes failure
- This check is essential for determining whether full partitionwise aggregation is possible versus requiring partial partitionwise aggregation

## Simplified Source

```c
static bool
group_by_has_partkey(RelOptInfo *input_rel, List *targetList, List *groupClause)
{
    // Get expressions from GROUP BY clause
    List *groupexprs = get_sortgrouplist_exprs(groupClause, targetList);

    // Early exit if no partition expressions exist
    if (!input_rel->partexprs)
        return false;

    int partnatts = input_rel->part_scheme->partnatts;

    // Check each partition key attribute
    for (int cnt = 0; cnt < partnatts; cnt++)
    {
        List *partexprs = input_rel->partexprs[cnt];
        bool found = false;

        // Try to match any partition expression for this key
        foreach(lc, partexprs)
        {
            Expr *partexpr = lfirst(lc);
            Oid partcoll = input_rel->part_scheme->partcollation[cnt];

            // Compare against each GROUP BY expression
            foreach(lg, groupexprs)
            {
                Expr *groupexpr = lfirst(lg);
                Oid groupcoll = exprCollation((Node *) groupexpr);

                // Handle RelabelType wrapper
                if (IsA(groupexpr, RelabelType))
                    groupexpr = ((RelabelType *) groupexpr)->arg;

                // Check for expression match
                if (equal(groupexpr, partexpr))
                {
                    // Verify collations match if both are valid
                    if (OidIsValid(partcoll) && OidIsValid(groupcoll) &&
                        partcoll != groupcoll)
                        return false;

                    found = true;
                    break;
                }
            }

            if (found)
                break;
        }

        // If any partition key is not found in GROUP BY, fail
        if (!found)
            return false;
    }

    return true;
}
```