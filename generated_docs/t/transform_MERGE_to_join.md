# transform_MERGE_to_join

## Location
[src/backend/optimizer/prep/prepjointree.c:168-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L168-L394)

## Overview
Transforms a MERGE statement's jointree to include the target relation by creating an appropriate join between the source and target relations.

## Definition

```c
void
transform_MERGE_to_join(Query *parse)
```
## Detailed Description
This function is responsible for converting a MERGE statement into a join operation that includes both the target and source relations. It analyzes the MERGE action list to determine the appropriate join type (INNER, LEFT, RIGHT, or FULL) based on the presence of different WHEN clauses:

- **INNER JOIN**: Used when only WHEN MATCHED actions exist
- **LEFT JOIN**: Used when WHEN NOT MATCHED BY SOURCE actions exist  
- **RIGHT JOIN**: Used when WHEN NOT MATCHED BY TARGET actions exist
- **FULL JOIN**: Used when both NOT MATCHED BY SOURCE and NOT MATCHED BY TARGET actions exist

The function creates a new RangeTblEntry for the join, constructs a JoinExpr that combines the target and source relations, and updates the query's jointree accordingly. It also handles nullability adjustments for variables that may be affected by the outer join conditions.

## Parameters / Member Variables
- : The Query structure representing the MERGE statement to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating RangeTblEntry, JoinExpr, RangeTblRef, FromExpr, NullTest)
  - [makeAlias](../m/makeAlias.md)
  - [makeFromExpr](../m/makeFromExpr.md)  
  - [makeWholeRowVar](../m/makeWholeRowVar.md)
  - [add_nulling_relids](../a/add_nulling_relids.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - rt_fetch
  - [make_and_qual](../m/make_and_qual.md)
  - foreach_node macro
  - [lappend](../l/lappend.md), linitial, list_make1, list_length
  - IsA macro
  - elog, Assert
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (in src/backend/optimizer/plan/planner.c:700)

## Notes and Other Information
- Only processes queries with commandType == CMD_MERGE, returns early otherwise
- Creates a synthetic join RTE with eref alias "*MERGE*"
- For trigger-updatable views, handles the expanded view subquery as the target
- Adds nulling relids to handle nullable variables in outer joins
- Optimizes by setting mergeJoinCondition to NULL when no NOT MATCHED BY SOURCE actions exist
- When NOT MATCHED BY SOURCE actions exist, adds "src IS NOT NULL" check to prevent incorrect results during recheck evaluation