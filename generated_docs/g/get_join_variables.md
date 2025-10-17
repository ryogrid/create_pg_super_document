# get_join_variables

## Location
[src/backend/utils/adt/selfuncs.c:4956-4983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4956-L4983)

## Overview
Analyzes both sides of a join clause by applying examine_variable() to each argument and determines the orientation (normal or reversed) of the join relative to the SpecialJoinInfo structure.

## Definition

```c
struct to describe the expression.
 *
 * Inputs:
 *	root: the planner info
 *	node: the expression tree to examine
 *	varRelid: see specs for restriction selectivity functions
 *
 * Outputs: *vardata is filled as follows:
 *	var: the input expression (with any binary relabeling stripped, if
 *		it is or contains a variable;
```
## Detailed Description
This function is essential for join selectivity estimation in PostgreSQL's query planner. It processes both arguments of a join clause by examining each side using  to extract statistical information about the variables involved. The function then determines whether the join clause follows the expected orientation relative to the join structure.

The function distinguishes between "normal" and "reversed" join clauses. A join is considered "normal" if it follows the pattern "lhs_var OP rhs_var" (left-hand side variable operator right-hand side variable), and "reversed" if it follows "rhs_var OP lhs_var". This orientation information is crucial for the planner to correctly apply join selectivity estimates and understand the relationship between the join clause and the overall join structure defined in SpecialJoinInfo.

The determination of join orientation is made by checking which side of the join each variable belongs to, using bitmap subset operations to compare the relations involved in each variable against the left-hand and right-hand sides defined in the SpecialJoinInfo structure.

## Parameters / Member Variables
- : Pointer to PlannerInfo structure containing planner context and information
- : List containing the two arguments of the join operator clause
- : Pointer to SpecialJoinInfo structure containing information about the join structure
- : Output parameter that receives statistical information about the first (left) variable
- : Output parameter that receives statistical information about the second (right) variable
- : Output parameter set to true if the join clause is in reversed orientation, false if normal

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - elog
  - linitial
  - lsecond
  - [examine_variable](../e/examine_variable.md)
  - [bms_is_subset](../b/bms_is_subset.md)
- Called from (representative examples):
  - [eqjoinsel](../e/eqjoinsel.md)
  - [neqjoinsel](../n/neqjoinsel.md)
  - [networkjoinsel](../n/networkjoinsel.md)

## Notes and Other Information
- The function expects exactly two arguments for the join operator and will throw an ERROR if this condition is not met
- When examining variables, the function passes 0 as the varRelid parameter, meaning variables from all relations are considered as variables (not pseudoconstants)
- The join orientation detection uses bitmap set operations to determine which side of the join each variable belongs to
- In complicated cases where the orientation cannot be definitively determined, the function defaults to considering the join as normal (not reversed)
- This function is primarily used by join selectivity estimation functions like eqjoinsel and neqjoinsel to understand the structure of join conditions

## Simplified Source

```c
void get_join_variables(PlannerInfo *root, List *args, SpecialJoinInfo *sjinfo,
                       VariableStatData *vardata1, VariableStatData *vardata2,
                       bool *join_is_reversed)
{
    Node *left, *right;

    // Extract the two join arguments
    if (list_length(args) != 2)
        elog(ERROR, "join operator should take two arguments");

    left = (Node *) linitial(args);
    right = (Node *) lsecond(args);

    // Examine both variables to extract statistical data
    examine_variable(root, left, 0, vardata1);
    examine_variable(root, right, 0, vardata2);

    // Determine join orientation based on which side each variable belongs to
    if (vardata1->rel &&
        bms_is_subset(vardata1->rel->relids, sjinfo->syn_righthand))
        *join_is_reversed = true;  // var1 is on RHS
    else if (vardata2->rel &&
             bms_is_subset(vardata2->rel->relids, sjinfo->syn_lefthand))
        *join_is_reversed = true;  // var2 is on LHS
    else
        *join_is_reversed = false; // Default to normal orientation
}
```