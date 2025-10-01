# determineRecursiveColTypes

## Location
[src/backend/parser/analyze.c:2334-2387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2334-L2387)

## Overview
Sets up column types for the parent CTE by processing the outputs of the non-recursive term of a recursive union. This function determines the column names and types that will be used for the entire recursive CTE.

## Definition

```c
static void
determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist)
```
## Detailed Description
This function is a critical component in the processing of recursive Common Table Expressions (CTEs). When PostgreSQL encounters a recursive CTE, it needs to determine the column structure based on the non-recursive term. The function finds the leftmost leaf SELECT statement in the set operation tree and uses its column names combined with the data types from the non-recursive term's target list to establish the CTE's output columns.

The process involves:
1. Traversing the set operation tree to find the leftmost leaf SELECT
2. Extracting the range table entry for that SELECT
3. Creating a dummy target list that combines column names from the leftmost SELECT with expressions from the non-recursive term
4. Using this information to analyze and set up the CTE's target list structure

This approach ensures that recursive CTEs have consistent column definitions across both recursive and non-recursive branches.

## Parameters / Member Variables
- : Parse state containing context information including the parent CTE and range table
- : Left argument node of the set operation, used to find the leftmost leaf SELECT
- : Target list from the non-recursive term, providing the expression types

## Dependencies
- Functions called/Symbols referenced:
  - [SetOperationStmt](../S/SetOperationStmt.md) (struct access)
  - [RangeTblRef](../R/RangeTblRef.md) (struct access)  
  - rt_fetch (retrieves range table entry)
  - forboth (macro for parallel list iteration)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target entry nodes)
  - [analyzeCTETargetList](../a/analyzeCTETargetList.md) (analyzes CTE target list structure)
- Called from (representative examples):
  - [transformSetOperationTree](../t/transformSetOperationTree.md)

## Notes and Other Information
This function is specifically designed for recursive CTE processing and is called during the parse analysis phase. It assumes the input represents a valid set operation structure where the leftmost leaf is a RangeTblRef. The function creates dummy target entries to facilitate type analysis without executing the actual queries. The resulting column information becomes part of the parent CTE's structure and is used for type checking throughout the recursive CTE processing.

## Simplified Source

```c
static void determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist) {
    Node *node;
    int leftmostRTI;
    Query *leftmostQuery;
    List *targetList;
    ListCell *left_tlist;
    ListCell *nrtl;
    int next_resno;

    // Find leftmost leaf SELECT in the set operation tree
    node = larg;
    while (node && IsA(node, SetOperationStmt))
        node = ((SetOperationStmt *) node)->larg;

    Assert(node && IsA(node, RangeTblRef));
    leftmostRTI = ((RangeTblRef *) node)->rtindex;
    leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;

    // Generate dummy targetlist combining leftmost SELECT column names
    // with non-recursive term expression types
    targetList = NIL;
    next_resno = 1;

    forboth(nrtl, nrtargetlist, left_tlist, leftmostQuery->targetList) {
        TargetEntry *nrtle = (TargetEntry *) lfirst(nrtl);
        TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);

        // Use column name from leftmost SELECT and expression from non-recursive term
        char *colName = pstrdup(lefttle->resname);
        TargetEntry *tle = makeTargetEntry(nrtle->expr, next_resno++, colName, false);
        targetList = lappend(targetList, tle);
    }

    // Analyze and set up the CTE's output column structure
    analyzeCTETargetList(pstate, pstate->p_parent_cte, targetList);
}
```