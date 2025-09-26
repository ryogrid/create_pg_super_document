# get_restriction_variable

## Location
src/backend/utils/adt/selfuncs.c: 4896 - 4955

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
  - list_length
  - linitial
  - lsecond
  - examine_variable
  - estimate_expression_value
  - ReleaseVariableStats
- Called from (representative examples):
  - eqsel_internal
  - generic_restriction_selectivity
  - scalarineqsel_wrapper
  - tsmatchsel
  - arraycontsel
  - patternsel_common
  - multirangesel
  - networksel
  - rangesel

## Notes and Other Information
- The function returns true only if exactly one side is a variable and the other is a pseudoconstant; if both sides are variables, it fails because callers expect the other side to act as a constant
- When  is nonzero, variables from other relations are treated as pseudoconstants, which is important for join selectivity estimation
- The function automatically cleans up variable statistics using  when the clause structure is invalid
- This is a fundamental utility used by many selectivity estimation functions across different data types and operators
- The function assumes binary operator clauses and will fail for clauses with other structures