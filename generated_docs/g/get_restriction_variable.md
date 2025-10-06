# get_restriction_variable

## Location
[src/backend/utils/adt/selfuncs.c:4896-4955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4896-L4955)

## Overview
Analyzes the arguments of a restriction clause to identify if it follows the pattern (variable op pseudoconstant) or (pseudoconstant op variable) and extracts information about the variable and the constant side.

## Definition

```c
structure (probably var op var) */
	ReleaseVariableStats(*vardata);
```
## Detailed Description
This function is a key component of PostgreSQL's selectivity estimation system. It examines the arguments of a restriction clause (typically a WHERE condition) to determine if the clause has the desired structure for statistical analysis: one side being a variable (which could be a column or expression involving columns from a single relation) and the other side being a pseudoconstant (a value that can be evaluated at planning time).

The function uses  to analyze both sides of the clause. If exactly one side is identified as a variable and the other as a pseudoconstant, the function succeeds and provides detailed information about the variable's statistics and the evaluated constant value. This information is crucial for the query planner to estimate how many rows will match the restriction condition.

The function handles the complexity of determining which expressions qualify as variables versus pseudoconstants, especially in the context of joins where  can restrict which relations' columns are considered as variables.

## Parameters / Member Variables
- : Pointer to PlannerInfo structure containing planner context and information
- : List containing the two arguments of the binary operator clause
- : Relation ID for restriction context; when nonzero, variables from other relations are treated as pseudoconstants
- : Output parameter that receives statistical information about the identified variable
- : Output parameter that receives the pseudoconstant argument, reduced to its estimated value
- : Output parameter set to true if variable is the left argument, false if right

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - linitial
  - lsecond
  - [examine_variable](../e/examine_variable.md)
  - [estimate_expression_value](../e/estimate_expression_value.md)
  - ReleaseVariableStats
- Called from (representative examples):
  - [eqsel_internal](../e/eqsel_internal.md)
  - [generic_restriction_selectivity](generic_restriction_selectivity.md)
  - [scalarineqsel_wrapper](../s/scalarineqsel_wrapper.md)
  - [tsmatchsel](../t/tsmatchsel.md)
  - [arraycontsel](../a/arraycontsel.md)
  - [patternsel_common](../p/patternsel_common.md)
  - [multirangesel](../m/multirangesel.md)
  - [networksel](../n/networksel.md)
  - [rangesel](../r/rangesel.md)

## Notes and Other Information
- The function returns true only if exactly one side is a variable and the other is a pseudoconstant; if both sides are variables, it fails because callers expect the other side to act as a constant
- When  is nonzero, variables from other relations are treated as pseudoconstants, which is important for join selectivity estimation
- The function automatically cleans up variable statistics using  when the clause structure is invalid
- This is a fundamental utility used by many selectivity estimation functions across different data types and operators
- The function assumes binary operator clauses and will fail for clauses with other structures

## Simplified Source

```c
bool get_restriction_variable(PlannerInfo *root, List *args, int varRelid,
                             VariableStatData *vardata, Node **other,
                             bool *varonleft) {
    Node *left, *right;
    VariableStatData rdata;

    // Must be a binary operation (two arguments)
    if (list_length(args) != 2)
        return false;

    left = (Node *) linitial(args);
    right = (Node *) lsecond(args);

    // Examine both sides to determine which are variables vs constants
    // varRelid context determines what counts as a variable
    examine_variable(root, left, varRelid, vardata);
    examine_variable(root, right, varRelid, &rdata);

    // Case 1: Left side is variable, right side is constant
    if (vardata->rel && rdata.rel == NULL) {
        *varonleft = true;
        *other = estimate_expression_value(root, rdata.var);
        return true;
    }

    // Case 2: Right side is variable, left side is constant
    if (vardata->rel == NULL && rdata.rel) {
        *varonleft = false;
        *other = estimate_expression_value(root, vardata->var);
        *vardata = rdata;  // Move right side data to output
        return true;
    }

    // Failed: either both sides are variables or both are constants
    ReleaseVariableStats(*vardata);
    ReleaseVariableStats(rdata);
    return false;
}
```