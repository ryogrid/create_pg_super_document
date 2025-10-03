# fix_indexorderby_references

## Location
[src/backend/optimizer/plan/createplan.c:5064-5092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5064-L5092)

## Overview
Adjusts indexorderby clauses to the form required by PostgreSQL's executor's index machinery, serving as a simplified version of fix_indexqual_references for ORDER BY expressions.

## Definition

```c
static List *
fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path)
```
## Detailed Description
This function performs transformations on index ORDER BY clauses to prepare them for execution by the index scan machinery. It is a simplified counterpart to fix_indexqual_references, designed specifically for handling ORDER BY expressions in index scans.

The function processes each ORDER BY clause in the index path by:
1. Extracting the clause and its corresponding index column number
2. Applying the same transformations as fix_indexqual_clause (parameter replacement and index key mapping)
3. Building a new list of fixed ORDER BY expressions ready for executor consumption

Unlike fix_indexqual_references, this function works with bare clauses and a separate indexcol list rather than IndexClause structures, reflecting the simpler structure of ORDER BY specifications compared to WHERE clause qualifications.

The function uses the forboth() macro to iterate through both the indexorderbys list and the indexorderbycols list simultaneously, ensuring proper pairing of ORDER BY expressions with their corresponding index column numbers.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and state information
- `*index_path`: IndexPath representing the index scan path with ORDER BY clauses to be processed
## Dependencies
- Functions called/Symbols referenced:
  - forboth (macro)
  - lfirst_int
  - [fix_indexqual_clause](fix_indexqual_clause.md)
  - [IndexPath](../I/IndexPath.md) (struct type)
  - [IndexOptInfo](../I/IndexOptInfo.md) (struct type)
- Called from (representative examples):
  - [create_indexscan_plan](../c/create_indexscan_plan.md)

## Notes and Other Information
This function is essential for index scans that need to maintain specific ordering, such as those used to satisfy ORDER BY clauses in queries. It ensures that ORDER BY expressions are properly transformed for execution while maintaining the correct association between expressions and index columns. The simplified design compared to fix_indexqual_references reflects the fact that ORDER BY clauses typically have simpler structure than WHERE clause qualifications. The function is part of the index scan planning infrastructure in PostgreSQL's query planner. Located in src/backend/optimizer/plan/createplan.c at lines 5064-5092.

## Simplified Source

```c
static List *fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path) {
    IndexOptInfo *index = index_path->indexinfo;
    List *fixed_indexorderbys = NIL;
    ListCell *lcc, *lci;

    // Process each ORDER BY clause with its corresponding index column
    forboth(lcc, index_path->indexorderbys, lci, index_path->indexorderbycols) {
        Node *clause = (Node *) lfirst(lcc);
        int indexcol = lfirst_int(lci);

        // Apply same transformations as indexqual clauses
        clause = fix_indexqual_clause(root, index, indexcol, clause, NIL);
        fixed_indexorderbys = lappend(fixed_indexorderbys, clause);
    }

    return fixed_indexorderbys;
}
```