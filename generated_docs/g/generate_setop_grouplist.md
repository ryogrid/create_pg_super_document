# generate_setop_grouplist

## Location
[src/backend/optimizer/prep/prepunion.c:1674-1706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1674-L1706)

## Overview
Builds a SortGroupClause list defining the sort/grouping properties of a set operation's output columns by copying parser-generated clauses and installing proper sortgrouprefs.

## Definition

```c
static List *
generate_setop_grouplist(SetOperationStmt *op, List *targetlist)
```
## Detailed Description
This function creates a properly configured list of SortGroupClause nodes for set operations by copying the grouping clauses that were determined during parse analysis and updating them with the correct sortgroupref values from the targetlist. The parser analysis determines the appropriate sorting and grouping properties for set operations but doesn't set the sortgrouprefs because the parser representation doesn't include targetlists for each setop node.

The function iterates through both the copied group clauses and the targetlist in parallel, matching each non-resjunk targetlist entry with its corresponding SortGroupClause and copying the ressortgroupref from the TargetEntry to the tleSortGroupRef field of the SortGroupClause. This establishes the proper linkage between the targetlist and the grouping/sorting specification.

## Parameters / Member Variables
- : SetOperationStmt containing the original groupClauses from parse analysis
- : targetlist for the set operation containing TargetEntry nodes with ressortgroupref values

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - list_head
  - [lnext](../l/lnext.md)
  - SetOperationStmt (structure type)
  - SortGroupClause (structure type)
- Called from:
  - [generate_recursion_path](generate_recursion_path.md)
  - [generate_union_paths](generate_union_paths.md)
  - [generate_nonunion_paths](generate_nonunion_paths.md)

## Notes and Other Information
- The function assumes that non-resjunk columns have ressortgroupref equal to their resno, which is a convention established by the targetlist generation functions
- Resjunk columns are skipped as they should not have sortgrouprefs and are not involved in grouping/sorting
- The function performs several assertions to verify the expected structure and consistency between the targetlist and group clauses
- The original group clauses from the parser have tleSortGroupRef set to 0, which this function updates with the actual references