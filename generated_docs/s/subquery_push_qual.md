# subquery_push_qual

## Location
[src/backend/optimizer/path/allpaths.c:3956-4002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3956-L4002)

## Overview
This function performs the actual pushdown of a previously validated restriction clause into a subquery, handling variable substitution and proper placement within the subquery structure.

## Definition
```c
static void subquery_push_qual(Query *subquery, RangeTblEntry *rte, Index rti, Node *qual)
```

## Detailed Description
This function is the execution phase of qualifier pushdown optimization - it actually pushes a restriction clause that has already been determined to be safe into the target subquery. The function handles two main scenarios:

**Set Operations**: When the subquery contains set operations (UNION, INTERSECT, EXCEPT), it delegates to recurse_push_qual() to recursively push the qualifier into each component query of the set operation tree.

**Regular Subqueries**: For standard subqueries without set operations, it performs variable substitution and clause placement:

1. **Variable Replacement**: Uses ReplaceVarsFromTargetList() to replace Var nodes in the qualifier (which reference subquery outputs) with copies of the corresponding targetlist expressions from the subquery. This ensures the pushed-down clause references the actual columns/expressions within the subquery context.

2. **Clause Placement**: Determines the appropriate location for the pushed qualifier:
   - **HAVING clause**: If the subquery uses aggregation, grouping, or already has a HAVING clause, the qualifier is added here since it logically applies to grouped results.
   - **WHERE clause**: For simple subqueries without aggregation/grouping, the qualifier goes in the WHERE clause (jointree->quals).

The function preserves the subquery's hasAggs and hasSubLinks flags since pushdown doesn't introduce new aggregates or subselects.

## Parameters / Member Variables
- `subquery`: The target subquery to receive the pushed-down qualifier
- `rte`: The RangeTblEntry for the subquery in the parent query
- `rti`: The range table index of the subquery in the parent query  
- `qual`: The restriction clause node to be pushed down

## Dependencies
- Functions called/Symbols referenced:
  - [recurse_push_qual](../r/recurse_push_qual.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [make_and_qual](../m/make_and_qual.md)
- Constants referenced:
  - REPLACEVARS_REPORT_ERROR
- Called from (representative examples):
  - [set_subquery_pathlist](set_subquery_pathlist.md) (src/backend/optimizer/path/allpaths.c:2572)
  - [recurse_push_qual](../r/recurse_push_qual.md) (src/backend/optimizer/path/allpaths.c:4013)

## Notes and Other Information
- Static function within allpaths.c, part of PostgreSQL's qualifier pushdown optimization framework
- Assumes the qualifier has already passed safety validation via qual_is_pushdown_safe()
- Handles both simple subqueries and complex set operation trees
- [Variable](../V/Variable.md) replacement ensures each component query in set operations gets its own copy of the qualifier
- Intelligent clause placement based on subquery structure (WHERE vs HAVING)
- Preserves query flags to maintain optimizer state consistency
- Located in src/backend/optimizer/path/allpaths.c:3956-4002

## Simplified Source

```c
static void
subquery_push_qual(Query *subquery, RangeTblEntry *rte, Index rti, Node *qual)
{
    if (subquery->setOperations != NULL)
    {
        // For set operations: recursively push to each component query
        recurse_push_qual(subquery->setOperations, subquery,
                         rte, rti, qual);
    }
    else
    {
        // Replace Vars in qual with subquery's targetlist expressions
        qual = ReplaceVarsFromTargetList(qual, rti, 0, rte,
                                       subquery->targetList,
                                       REPLACEVARS_REPORT_ERROR, 0,
                                       &subquery->hasSubLinks);

        // Place qual in appropriate location
        if (subquery->hasAggs || subquery->groupClause ||
            subquery->groupingSets || subquery->havingQual)
        {
            // Add to HAVING clause for aggregated queries
            subquery->havingQual = make_and_qual(subquery->havingQual, qual);
        }
        else
        {
            // Add to WHERE clause for simple queries
            subquery->jointree->quals =
                make_and_qual(subquery->jointree->quals, qual);
        }
    }
}
```