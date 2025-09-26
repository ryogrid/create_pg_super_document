# create_group_path

## Location
[src/backend/optimizer/util/pathnode.c:3044-3102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3044-L3102)

## Overview
Creates a pathnode that represents performing grouping of presorted input data, typically used to implement GROUP BY operations.

## Definition
```c
GroupPath *create_group_path(PlannerInfo *root,
                            RelOptInfo *rel,
                            Path *subpath,
                            List *groupClause,
                            List *qual,
                            double numGroups)
```

## Detailed Description
This function creates a GroupPath node that represents a grouping operation on data that is already sorted by the grouping columns. The Group node processes presorted input by scanning consecutive rows that have the same values for the grouping columns, forming groups without requiring additional sorting. This is an efficient implementation of GROUP BY when the input is appropriately ordered.

The function initializes the path structure, sets up grouping-specific fields, calculates costs using `cost_group`, and adds the target list evaluation costs for computing the output expressions. The resulting path preserves the sort ordering of the input since grouping does not change the relative order of groups.

## Parameters / Member Variables
- `root`: PlannerInfo containing planning context and optimizer settings
- `rel`: RelOptInfo representing the parent relation associated with the result
- `subpath`: Path representing the source of presorted input data
- `groupClause`: List of SortGroupClause structures representing the grouping specification
- `qual`: List of HAVING qualification expressions to be applied, if any
- `numGroups`: Estimated number of groups that will be produced by the grouping operation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates GroupPath node)
  - [cost_group](cost_group.md) (calculates grouping operation costs)
  - [list_length](../l/list_length.md) (counts grouping columns)
  - [GroupPath](../G/GroupPath.md) (return type structure)
  - [PathTarget](../P/PathTarget.md) (for target list evaluation)
- Called from (representative examples):
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)
  - [create_partial_grouping_paths](create_partial_grouping_paths.md)

## Notes and Other Information
- Requires input to be presorted by the grouping columns for efficient operation
- Preserves the sort ordering of the input since grouping maintains relative order of groups
- Includes costs for both the grouping operation itself and target list evaluation
- Assumes operation above joins (no parameterization) and inherits parallel safety from subpath
- HAVING qualifications are applied during the grouping process to filter groups
- The pathtype is set to T_Group to distinguish it from aggregation operations
- This is typically used when grouping without aggregation or with simple aggregation that can be handled by the Group node